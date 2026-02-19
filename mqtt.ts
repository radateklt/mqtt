/**
 * MQTT Broker/Connection
 * @version 1.2.0
 * @package @radatek/mqtt
 * @copyright Darius Kisonas 2023
 * @license MIT
 */

import {EventEmitter} from 'events'
import net from 'net'
import tls from 'tls'
import fs from 'fs'
import { nextTick } from 'process'

const BROKER_VERSION = 'MQTT-Broker'

const ERROR_UNSPECIFIED = 0x80
const ERROR_MALFORMED_PACKET = 0x81
const ERROR_PROTOCOL = 0x82
const ERROR_CLIENT_IDENTIFIER = 0x85
const ERROR_BAD_USERNAME_PASSWORD = 0x86
const ERROR_NOT_AUTHORIZED = 0x87
const ERROR_TOPIC_INVALID = 0x90
const ERROR_MESSAGEID_INUSE = 0x91
const ERROR_IDENTIFER_NOT_FOUND = 0x92

/*
supported MQTT Broker features:
  MQTT 3.1.1, 5.0
  connect: username, password, will, clean, keepalive, sessionExpiryInterval
  publish: qos=0-2, retain
  puback:
  subscribe: qos=0-2, rh (Retain Handling), nl (No Local), rap (Retain as Published)
  unsubscribe:
  policy: permissions, topic prefix, init subscriptions, init publications
*/

export declare interface MqttMessage {
  // @ connect, disconnect
  protocolVersion?: number
  /** all */
  cmd?: string

  /** connect */
  protocol?: string
  /** connect */
  clientId?: string
  /** connect */
  will?: {
    topic?: string,
    payload?: string | Buffer | number,
    qos?: number,
    retain?: boolean,
    properties?: MqttProperties
  }
  /** connect */
  username?: string
  /** connect */
  password?: string | Buffer
  /** connect */
  clean?: boolean
  /** connect */
  sessionPresent?: boolean
  /** connect */
  keepalive?: number

  /** publish */
  topic?: string
  /** publish */
  payload?: any
  /** publish */
  dup?: boolean
  /** publish */
  qos?: number
  /** publish */
  retain?: boolean

  /** subscribe */
  subscriptions?: Array<string | MqttSubscription>
  /** unsubscribe */
  unsubscriptions?: string[]
  /** suback, unsuback */
  granted?: number[]

  /** publish, puback, pubrec, pubrel, pubcomp, suback */
  messageId?: number
  /** connack, puback, pubrec, pubrel, pubcomp, suback, disconnect */
  reasonCode?: number
  /** connack v3.1.1 */
  returnCode?: number

  /** connect */
  properties?: MqttProperties

  /** @internal */
  intern?: boolean
  /** @internal */
  clients?: {[clientId: string]: number}
}

export declare interface ConnectMessage {
  protocolVersion?: number

  protocol?: string
  clientId?: string
  will?: {
    topic?: string,
    payload?: string | Buffer | number,
    qos?: number,
    retain?: boolean,
    properties?: MqttProperties
  }
  username?: string
  password?: string | Buffer
  clean?: boolean
  sessionPresent?: boolean
  keepalive?: number

  properties?: MqttProperties
}

export declare interface PublishMessage {
  messageId?: number
  topic?: string
  payload?: any
  dup?: boolean
  qos?: number
  retain?: boolean
  intern?: boolean
}

export declare interface MqttSubscription {
  topic: string
  qos?: number
  rh?: number
  nl?: boolean
  rap?: boolean
}

export declare interface MqttProperties {
  payloadFormatIndicator?: number
  messageExpiryInterval?: number
  contentType?: string
  responseTopic?: string
  correlationData?: number
  subscriptionIdentifier?: number
  sessionExpiryInterval?: number
  assignedClientIdentifier?: string
  authenticationMethod?: string
  authenticationData?: Buffer | string
  requestProblemInformation?: number
  willDelayInterval?: number
  requestResponseInformation?: number
  responseInformation?: string
  serverReference?: string
  reasonString?: string
  receiveMaximum?: number
  topicAliasMaximum?: number
  topicAlias?: string
  maximumQoS?: number
  retainAvailable?: boolean
  userProperties?: {[key: string]: string}
  maximumPacketSize?: number
  wildcardSubscriptionAvailable?: boolean
  subscriptionIdentifiersAvailable?: boolean
  sharedSubscriptionAvailable?: boolean
}

const MQTT_COMMANDS: string[] = ',connect,connack,publish,puback,pubrec,pubrel,pubcomp,subscribe,suback,unsubscribe,unsuback,pingreq,pingresp,disconnect,auth'.split(',')
const EMPTY_BUFFER: Buffer = Buffer.alloc(0)

// @internal
class PayloadReader {
  _buffer: Buffer
  _idx: number = 0
  constructor(buffer: Buffer, idx: number = 0) {
    this._buffer = buffer
    this._idx = 0
  }

  _readUIntVar(): number {
    let i = this._idx, v:number = 0, bits: number = 0, b: number
    do {
      if (i >= this._buffer.length)
        throw new Error('Invalid input')
      b = this._buffer[i++]
      v |= (b & 0x7f) << bits
      bits += 7
    } while (b & 0x80)
    this._idx = i
    return v
  }

  _readUInt8(): number {
    if (this._idx >= this._buffer.length)
      throw new Error('Invalid input')
    return this._buffer[this._idx++]
  }

  _readUInt16(): number {
    const i = this._idx
    if ((this._idx += 2) > this._buffer.length)
      throw new Error('Invalid input')
    return this._buffer.readUInt16BE(i)
  }

  _readUInt32() {
    const i = this._idx
    if ((this._idx += 4) > this._buffer.length)
      throw new Error('Invalid input')
    return this._buffer.readUInt32BE(i)
  }

  _readString() {
    return this._readBuffer().toString('utf8')
  }

  _readBuffer() {
    const len: number = this._readUInt16()
    const i: number = this._idx
    if ((this._idx += len) > this._buffer.length)
      throw new Error('Invalid input')
    return this._buffer.subarray(i, this._idx)
  }
}
class PayloadWriter {
  // @internal
  private _list: any[] = []
  constructor() {
    this._list.push([0, '1', 0], [0, 'v', []])
  }

  _add(type: string, value?: any, idx?: number) {
    let size: number = 0
    switch (type) {
      case 'b':
        size = 1
        break
      case '1':
      case '2':
      case '4':
        size = +type
        break
      case 'v':
        if (value === undefined) {
          size = -1
          value = []
        } else {
          let n: number = value
          value = n ? [] : [0]
          while (n) {
            const b: number = n & 0x7f
            n >>>= 7
            value.push((n ? 0x80 : 0) | b)
          }
          size = value.length
        }
        break
      case 'B':
      case 's':
        size = Buffer.byteLength(value||'') + 2
        if (size > 32769)
          throw new Error('Invalid data')
        break
      case 'R':
        size = Buffer.byteLength(value)
        break
    }
    idx = idx ?? (this._list.push([]) - 1)
    const item: any[] = this._list[idx]
    item[0] = size
    item[1] = type
    item[2] = value
  }

  _addUInt8(value: number | boolean, idx?: number) {
    this._add('1', value, idx)
  }

  _addUInt16(value: number) {
    this._add('2', value)
  }

  _addUInt32(value: number) {
    this._add('4', value)
  }

  _addUIntVar(value: number) {
    this._add('v', value)
  }

  _addString(value: string | Buffer | number | undefined) {
    if (value === undefined)
      value = ''
    if (typeof value === 'number')
      value = value.toString()
    this._add(value instanceof Buffer ? 'B' : 's', value)
  }

  _addBuffer(value: Buffer) {
    this._add('B', value)
  }

  _addRaw(value: string | Buffer) {
    this._add('R', value)
  }

  _updateSize() {
    return this._list.reduceRight((size: number, item: any, index: number) => {
      if (item[0] === -1)
        this._add(item[1], size, index)
      return size + item[0]
    }, 0)
  }

  _toBuffer(): Buffer {
    if (this._list[0][0] !== 1)
      throw new Error('Missing header')
    if (!this._list[1][0])
      this._list[1][0] = -1
    const size: number = this._updateSize()
    const buf: Buffer = Buffer.allocUnsafe(size)
    this._list.reduce((pos: number, item: {0: number, 1: string, 2: any}) => {
      const v: any = item[2]
      switch (item[1]) {
        case 'b':
        case '1':
          buf.writeUInt8(v, pos)
          break
        case '2':
          buf.writeUInt16BE(v, pos)
          break
        case '4':
          buf.writeUInt32BE(v, pos)
          break
        case 'v':
          buf.set(v, pos)
          break
        case 'B':
        case 's':
          buf.writeUInt16BE((item[0] as number) - 2, pos)
          if (v instanceof Buffer)
            v.copy(buf, pos + 2)
          else
            buf.write(v || '', pos + 2, 'utf8')
          break
        case 'R':
          if (v instanceof Buffer)
            v.copy(buf, pos)
          else
            buf.write(v || '', pos, 'utf8')
          break
      }
      return pos + item[0]
    }, 0)
    return buf
  }
}

const MqttDecodePropTypes:string = '-14s----sB-v-----4-s-sB141s-s--s-22s1bs4bbb'
const MqttDecodeProp:string[] = ',payloadFormatIndicator,messageExpiryInterval,contentType,,,,,responseTopic,correlationData,,subscriptionIdentifier,,,,,,sessionExpiryInterval,,assignedClientIdentifier,,authenticationMethod,authenticationData,requestProblemInformation,willDelayInterval,requestResponseInformation,responseInformation,,serverReference,,,reasonString,,receiveMaximum,topicAliasMaximum,topicAlias,maximumQoS,retainAvailable,userProperties,maximumPacketSize,wildcardSubscriptionAvailable,subscriptionIdentifiersAvailable,sharedSubscriptionAvailable'.split(',')

function decodeMqttProperties(obj: any, payload: PayloadReader, filter?: number[]) {
  if (payload._idx >= payload._buffer.length)
    return
  const last: number = payload._readUIntVar() + payload._idx
  if (last > payload._idx)
    obj = obj.properties = obj.properties || {}
  while (payload._idx < last) {
    const id: number = payload._readUInt8()
    if (filter && filter.indexOf(id) < 0)
      throw new Error('Invalid property')
    const n: string = MqttDecodeProp[id] || ''
    let v: any
    switch(MqttDecodePropTypes[id]) {
      case '1':
        v = payload._readUInt8()
        break
      case '2':
        v = payload._readUInt16()
        break
      case '4':
        v = payload._readUInt32()
        break
      case 'v':
        v = payload._readUIntVar()
        break
      case 's':
        v = payload._readString()
        break
      case 'B':
        v = payload._readBuffer()
        break
      case 'b':
        v = !!payload._readUInt8()
        break
    }
    if (n)
      if (n === 'userProperties') {
        obj.userProperties = obj.userProperties || {}
        obj.userProperties[v] = payload._readString()
      } else
        obj[n] = v
  }
}

const MqttDecode: {[id: string]: (msg: MqttMessage, payload: PayloadReader, mqtt5: boolean) => void} = {
  connect(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.protocol = payload._readString()
    msg.protocolVersion = payload._readUInt8()
    mqtt5 = msg.protocolVersion === 5
    let flags: number = payload._readUInt8()
    msg.clean = !!(flags & 0x2)
    if (flags & 0x04) {
      msg.will = {
        qos: (flags >> 3) & 3,
        retain: !!(flags & 0x20)
      }
    }
    msg.keepalive = payload._readUInt16()
    if (mqtt5)
      decodeMqttProperties(msg, payload, [0x11, 0x15, 0x16, 0x17, 0x19, 0x21, 0x22, 0x26, 0x27])
    msg.clientId = payload._readString()
    if (msg.will) {
      if (mqtt5)
        decodeMqttProperties(msg.will, payload, [0x01, 0x02, 0x03, 0x08, 0x09, 0x18, 0x26])
      msg.will.topic = payload._readString()
      msg.will.payload = payload._readString()
    }
    if (flags & 0x80)
      msg.username = payload._readString()
    if (flags & 0x40)
      msg.password = payload._readString()
  },
  connack(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.sessionPresent = !!(payload._readUInt8() & 1)
    msg.reasonCode = payload._readUInt8()
    if (!mqtt5)
      msg.returnCode = msg.reasonCode
    decodeMqttProperties(msg, payload, [0x11, 0x12, 0x13, 0x15, 0x16, 0x1A, 0x1C, 0x1F, 0x21, 0x22, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A])
  },
  publish(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.topic = payload._readString()
    if (msg.qos)
      msg.messageId = payload._readUInt16()
    if (mqtt5)
      decodeMqttProperties(msg, payload, [0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x23, 0x26])
    msg.payload = payload._buffer.subarray(payload._idx)
  },
  puback(msg: MqttMessage, payload: PayloadReader): void {
    msg.messageId = payload._readUInt16()
    if (payload._idx < payload._buffer.length)
      msg.reasonCode = payload._readUInt8()
    decodeMqttProperties(msg, payload, [0x1F, 0x26])
  },
  pubrec(msg: MqttMessage, payload: PayloadReader): void {
    msg.messageId = payload._readUInt16()
    if (payload._idx < payload._buffer.length)
      msg.reasonCode = payload._readUInt8()
    decodeMqttProperties(msg, payload, [0x1F, 0x26])
  },
  pubrel(msg: MqttMessage, payload: PayloadReader): void {
    msg.messageId = payload._readUInt16()
    if (payload._idx < payload._buffer.length)
      msg.reasonCode = payload._readUInt8()
    decodeMqttProperties(msg, payload, [0x1F, 0x26])
  },
  pubcomp(msg: MqttMessage, payload: PayloadReader): void {
    msg.messageId = payload._readUInt16()
    if (payload._idx < payload._buffer.length)
      msg.reasonCode = payload._readUInt8()
    decodeMqttProperties(msg, payload, [0x1F, 0x26])
  },
  subscribe(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.messageId = payload._readUInt16()
    if (mqtt5)
      decodeMqttProperties(msg, payload, [0x0B, 0x26])
    msg.subscriptions = []
    while (payload._idx < payload._buffer.length) {
      const topic: string = payload._readString()
      const flag: number = payload._readUInt8()
      msg.subscriptions.push({
        topic,
        qos: flag & 3,
        nl: !!(flag & 4),
        rap: !!(flag & 8),
        rh: (flag >> 4) & 3
      })
    }
  },
  suback(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.messageId = payload._readUInt16()
    if (mqtt5)
      decodeMqttProperties(msg, payload, [0x1F, 0x26])
    msg.granted = []
    while (payload._idx < payload._buffer.length)
      msg.granted.push(payload._readUInt8())
  },
  unsubscribe(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.messageId = payload._readUInt16()
    if (mqtt5)
      decodeMqttProperties(msg, payload, [0x26])
    msg.unsubscriptions = []
    while (payload._idx < payload._buffer.length)
      msg.unsubscriptions.push(payload._readString())
  },
  unsuback(msg: MqttMessage, payload: PayloadReader, mqtt5: boolean): void {
    msg.messageId = payload._readUInt16()
    if (mqtt5)
      decodeMqttProperties(msg, payload, [0x1F, 0x26])
    msg.granted = []
    while (payload._idx < payload._buffer.length)
      msg.granted.push(payload._readUInt8())
  },
  pingreq(): void {},
  pingresp(): void {},
  disconnect(msg: MqttMessage, payload: PayloadReader): void {
    msg.reasonCode = 0
    if (payload._idx < payload._buffer.length)
      msg.reasonCode = payload._readUInt8()
    decodeMqttProperties(msg, payload, [0x11, 0x1C, 0x1F, 0x26])
  },
  auth(msg: MqttMessage, payload: PayloadReader): void {
    msg.reasonCode = payload._readUInt8()
    decodeMqttProperties(msg, payload, [0x15, 0x16, 0x1F, 0x26])
  }
}

function encodeMqttProperties(obj: any, payload: PayloadWriter, filter: number[]) {
  if (!obj.properties) {
    payload._add('v', 0)
    return
  }
  payload._add('v')
  obj = obj.properties
  filter.forEach((id: number) => {
    const n: string = MqttDecodeProp[id] || ''
    if (n && n in obj) {
      if (n === 'userProperties') {
        for (const k in obj.userProperties) {
          payload._add('1', id)
          payload._add('s', k)
          payload._add('s', obj.userProperties[k].toString())
        }
      } else {
        payload._add('1', id)
        payload._add(MqttDecodePropTypes[id], obj[n])
      }
    }
  })
  payload._updateSize()  
}

const MqttEncode: {[id: string]: (msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean) => void} = {
  connect(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (msg.protocolVersion === undefined)
      msg.protocolVersion = mqtt5 ? 5 : 4
    mqtt5 = msg.protocolVersion === 5
    if (!msg.clientId)
      msg.clientId = 'client-' + Math.random().toString(36).slice(2)

    payload._addUInt8(0x10, 0)
    payload._addString('MQTT')
    payload._addUInt8(msg.protocolVersion)
    payload._addUInt8(
      (msg.clean ? 0x02 : 0) |
      (msg.will?.topic ? (0x04 | (msg.will.qos || 0) << 3 | (msg.will.retain ? 0x20 : 0)) : 0) |
      (msg.password ? 0x40 : 0) |
      (msg.username ? 0x80 : 0)     
    )
    payload._addUInt16(msg.keepalive || 120)
    if (mqtt5)
      encodeMqttProperties(msg, payload, [0x11, 0x15, 0x16, 0x17, 0x19, 0x21, 0x22, 0x26, 0x27])
    payload._addString(msg.clientId)
    if (msg.will) {
      if (mqtt5)
        encodeMqttProperties(msg.will, payload, [0x01, 0x02, 0x03, 0x08, 0x09, 0x18, 0x26])
      payload._addString(msg.will.topic || '')
      payload._addString(msg.will.payload)
    }
    if (msg.username)
      payload._addString(msg.username)
    if (msg.password)
      payload._addString(msg.password)
  },
  connack(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    payload._addUInt8(0x20, 0)
    payload._addUInt8(msg.sessionPresent ? 1 : 0)
    if (!mqtt5) {
      let reasonCode: number = msg.reasonCode || msg.returnCode || 0
      switch (reasonCode) {
        case 0:
          break
        case ERROR_PROTOCOL:
          reasonCode = 1
          break
        case ERROR_CLIENT_IDENTIFIER:
          reasonCode = 2
          break
        case ERROR_BAD_USERNAME_PASSWORD:
          reasonCode = 4
          break
        default:
          reasonCode = 5
          break
      }
      payload._addUInt8(reasonCode)
    } else {
      payload._addUInt8(msg.reasonCode || 0)
      encodeMqttProperties(msg, payload, [0x11, 0x15, 0x16, 0x17, 0x19, 0x21, 0x22, 0x26, 0x27])
    }
  },
  publish(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    payload._addUInt8(0x30 | ((msg.qos || 0) << 1) | (msg.dup ? 0x08 : 0) | (msg.retain ? 0x01 : 0), 0)
    payload._addString(msg.topic)
    if (msg.qos) {
      if (!msg.messageId)
        throw new Error('Missing messageId')
      payload._addUInt16(msg.messageId)
    }
    if (mqtt5)
      encodeMqttProperties(msg, payload, [0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x23, 0x26])
    payload._addRaw(msg.payload)
  },
  puback(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0x40, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5) {
      payload._addUInt8(msg.reasonCode || 0)
      encodeMqttProperties(msg, payload, [0x1F, 0x26])
    }
  },
  pubrec(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0x50, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5) {
      payload._addUInt8(msg.reasonCode || 0)
      encodeMqttProperties(msg, payload, [0x1F, 0x26])
    }
  },
  pubrel(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0x62, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5) {
      payload._addUInt8(msg.reasonCode || 0)
      encodeMqttProperties(msg, payload, [0x1F, 0x26])
    }
  },
  pubcomp(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0x70, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5) {
      payload._addUInt8(msg.reasonCode || 0)
      encodeMqttProperties(msg, payload, [0x1F, 0x26])
    }
  },
  subscribe(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.subscriptions)
      throw new Error('Missing properties')
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0x82, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5)
      encodeMqttProperties(msg, payload, [0x0B, 0x26])
    msg.subscriptions.forEach((sub: string | MqttSubscription) => {
      if (typeof sub === 'string')
        sub = {topic: sub, qos: msg.qos || 0, rap: msg.retain}
      payload._addString(sub.topic || '')
      payload._addUInt8((sub.qos || 0) | (sub.rh || 0) << 4 | (sub.rap ? 8 : 0) | (sub.nl ? 4 : 0))
    })
  },
  suback(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0x90, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5)
      encodeMqttProperties(msg, payload, [0x1F, 0x26])
    msg.granted?.forEach((reesultCode: number) => payload._addUInt8(reesultCode))
  },
  unsubscribe(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.unsubscriptions)
      throw new Error('Missing properties')
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0xA2, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5)
      encodeMqttProperties(msg, payload, [0x26])
    msg.unsubscriptions.forEach((topic: string) => payload._addString(topic))
  },
  unsuback(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!msg.messageId)
      throw new Error('Missing messageId')
    payload._addUInt8(0xB0, 0)
    payload._addUInt16(msg.messageId)
    if (mqtt5) {
      encodeMqttProperties(msg, payload, [0x1F, 0x26])
      msg.granted?.forEach((reesultCode: number) => payload._addUInt8(reesultCode))
    }
  },
  pingreq(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    payload._addUInt8(0xC0, 0)
  },
  pingresp(msg: MqttMessage, payload: PayloadWriter): void {
    payload._addUInt8(0xD0, 0)
  },
  disconnect(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    payload._addUInt8(0xE0, 0)
    if (mqtt5) {
      payload._addUInt8(msg.reasonCode || 0)
      encodeMqttProperties(msg, payload, [0x11, 0x1C, 0x1F, 0x26])
    }
  },
  auth(msg: MqttMessage, payload: PayloadWriter, mqtt5: boolean): void {
    if (!mqtt5)
      throw new Error('Invalid data')
    payload._addUInt8(0xF0, 0)
    payload._addUInt8(msg.reasonCode || 0)
    encodeMqttProperties(msg, payload, [0x15, 0x16, 0x1F, 0x26])
  }
}

export class MqttGenerator {
  public protocolVersion: number
  constructor(protocolVersion: number = 5) {
    if (protocolVersion < 4 || protocolVersion > 5)
      throw new Error('Unsupported protocol version ' + protocolVersion)
    this.protocolVersion = protocolVersion
  }

  generate(msg: MqttMessage): Buffer {
    const payload: PayloadWriter = new PayloadWriter()
    const gen = MqttEncode[msg.cmd || '']
    if (!gen)
      throw new Error('Invalid data')
    gen(msg, payload, this.protocolVersion === 5)
    return payload._toBuffer()  
  }
}

export class MqttParser extends EventEmitter {
  public protocolVersion: number
  // @internal
  private _list: Buffer[] = []
  // @internal
  private _size: number = 0
  // @internal
  private _lastSize: number = 0
  // @internal
  private _lastHdr: number = 0

  constructor(protocolVersion: number = 5) {
    super()
    this.protocolVersion = protocolVersion
  }

  generate(msg: MqttMessage): Buffer {
    try {
      return new MqttGenerator(this.protocolVersion).generate(msg)
    } catch (e) {
      this.emit('error', e)
      throw e
    }
  }

  parse(data: Buffer): boolean {
    if (this._lastSize < 0)
      return false
    this._list.push(data)
    this._size += data.length

    try {
      let msg: MqttMessage | undefined
      while (msg = this._getPacket())
        this.emit('packet', msg)
      return true
    } catch (e) {
      this.emit('error', e)
      return false
    }
  }

  clear() {
    this._list =[]
    this._size = 0
    this._lastSize = 0
    this._lastHdr = 0
  }

  // @internal
  private _hasPacket(): boolean {
    let buf: Buffer
    let size: number = this._lastSize
    let bits: number = 0
    if (size)
      return size <= this._size
    for (let top = 0, idx = 1, hdr = 2; buf = this._list[top]; top++, idx = 0) {
      for (let n = buf.length; idx < n; idx++, hdr++) {
        const b = buf[idx]
        size = size | ((b & 0x7F) << bits)
        bits += 7
        if (!(b & 0x80)) {
          size += hdr
          if (hdr > 5) {
            this._lastSize = -1
            throw new Error('Invalid input')
          }
          this._lastHdr = hdr
          this._lastSize = size
          return size <= this._size
        }
      }
    }
    return false
  }

  // @internal
  private _getPacket(): MqttMessage | undefined {
    if (this._hasPacket()) {
      let size: number = this._lastSize, top: number = 0
      for (; size > 0; top++)
        size -= this._list[top].length

      if (top > 0) {
        const buf: Buffer = Buffer.concat(this._list.splice(0, top + 1, EMPTY_BUFFER))
        this._list[0] = buf
      }

      const header: number = this._list[0][0]
      const payload: Buffer = this._list[0].subarray(this._lastHdr, this._lastSize)
      this._list[0] = this._list[0].subarray(this._lastSize)
      this._size -= this._lastSize
      this._lastSize = 0
      if (!this._list[0].length)
        this._list.shift()

      const msg: MqttMessage = {}
      msg.cmd = MQTT_COMMANDS[header >> 4]
      if (msg.cmd === 'publish') {
        msg.dup = !!((header >> 3) & 0x1)
        msg.qos = (header >> 1) & 0x3
        msg.retain = !!(header & 0x1)
      }
      MqttDecode[msg.cmd](msg, new PayloadReader(payload), this.protocolVersion === 5)
      if (msg.cmd === 'connect')
        this.protocolVersion = msg.protocolVersion as number
      return msg
    }
  }
}

export declare interface TopicCollectionOptions {
  maxItems?: number
  maxSubscriptions?: number
  systemFilter?: boolean
}

export declare interface TopicItem {
  id?: string
  qos?: number
  publish?: boolean
  retain?: boolean
  topic?: string
  //payload?: string | Buffer | number
}

export declare interface TopicSubscription<T> {
  topic?: string
  items?: T[]
  sub?: {[key: string]: TopicSubscription<T>}
}

export class TopicCollection<T> {
  options: TopicCollectionOptions
  size: number = 0
  all: {[key: string]: TopicSubscription<T>} = {}
  sub: TopicSubscription<T> = {sub: {}}

  constructor (options: TopicCollectionOptions) {
    this.options = options || {}
  }

  /** Clear all data */
  clear () {
    this.size = 0
    this.all = {}
    this.sub = {sub: {}}
  }

  /** Add item object to topic */
  add (topic: string, item: T): T {
    if (topic.startsWith('/'))
      topic = topic.slice(1)
    let obj: TopicSubscription<T> | undefined = this.sub, sub: TopicSubscription<T> | undefined = this.all[topic]
    if (!sub) {
      for (const name of topic.split('/')) {
        sub = obj.sub?.[name]
        if (!sub) {
          obj.sub = obj.sub || {}
          sub = obj.sub[name] = {}
        }
        obj = sub
      }
    }
    if (!sub.items) {
      sub.items = []
      sub.topic = topic
      this.all[topic] = sub
    }
    if ((item as any).id) {
      for (let i = 0, n = sub.items.length; i < n; i++) {
        if ((sub.items[i] as any).id === (item as any).id) {
          sub.items[i] = item
          return item
        }
      }
    }
    this.size++
    sub.items.push(item)
    return item
  }

  /** Remove item object from topic */
  remove (topic: string, item?: string|((item: T) => boolean)): boolean {
    if (topic.startsWith('/'))
      topic = topic.slice(1)
    const sub = this.all[topic]
    let result: boolean = false
    if (sub && sub.items && sub.items.length) {
      if (item === undefined) {
        result = true
        this.size -= sub.items.length
        sub.items = []
      } else {
        const cb = typeof item === 'function' ? item : (obj: T): boolean => (obj as any).id === item
        sub.items = sub.items.filter((value: T): boolean => {
          const r: boolean = cb(value)
          result ||= r
          this.size--          
          return !r
        })
      }
    }
    return result
  }

  /** Get first item for topic */
  get (topic: string): T | undefined {
    if (topic.startsWith('/'))
      topic = topic.slice(1)
    const sub = this.all[topic]
    if (sub && sub.items)
      return sub.items[0]
  }

  /** Set first item for topic */
  set (topic: string, item: T): T {
    if (topic.startsWith('/'))
      topic = topic.slice(1)
    const sub = this.all[topic]
    if (!sub || !sub.items || !sub.items[0])
      this.add(topic, item)
    else
      sub.items[0] = item
    return item
  }

  /** Iterate items using topic wildcard: # - all items and subitems, + - all items the same level */
  iterateWildcard (topic: string, cb: (item: T, topic: string) => boolean) {
    if (topic.startsWith('/'))
      topic = topic.slice(1)
    const sub = this.all[topic]
    const systemFilter = this.options.systemFilter // skip wildcard on $... topics
    let topicList: string[]
    function iter (sub: TopicSubscription<T>, idx: number): boolean {
      const name = topicList[idx]
      if (sub && name) {
        if (name === '#') {
          // iterate all
          if (sub.items && sub.items.some((item: T) => cb(item, sub.topic || '')))
            return true
        }
        if (sub.sub) {
          if (name === '#' || name === '+') {
            for (const n in sub.sub) {
              if (systemFilter && (idx !== 0 || !n.startsWith('$')))
                if (iter(sub.sub[n], idx + (name === '+' ? 1 : 0)))
                  return true
            }
          } else
            return iter(sub.sub[name], idx + 1)
        }
      }
      return false
    }
    if (sub) {
      if (sub.items)
        sub.items.some((item: T) => cb(item, topic))
    } else {
      if (!topic.match(/[+#]/))
        return
      topicList = topic.split('/')
      iter(this.sub, 0)
    }
  }

  /** Iterate all items */
  iterate (topic: string, cb: (item: T, topic?: string) => boolean): boolean {
    if (topic.startsWith('/'))
      topic = topic.slice(1)
    const internal = this.options.systemFilter && topic.startsWith('$') // skip wildcard on $... topics
    const topicList = topic.split('/')
    const iter = (sub: TopicSubscription<T>, idx: number): boolean => {
      if (sub) {
        const name = topicList[idx]
        if (name === undefined)
          return sub.items?.some?.(item => cb(item, topic)) || false
        if (sub.sub) {
          if (!internal || sub !== this.sub)
            iter(sub.sub['#'], -1)
          iter(sub.sub['+'], idx+1)
          return iter(sub.sub[name], idx+1)
        }
      }
      return false
    }
    return iter(this.sub, topic[0] ? 0 : 1)
  }

  /** Get item[name] value from first matched topic */
  getOption (topic: string, name: string, def: any): any {
    let value: any = def === undefined ? true : def
    this.iterate(topic, (option: T): boolean => {
      const v = (option as any)[name]
      if (v !== undefined) {
        value = v
        return true
      }
      return false
    })
    return value
  }
}

export declare interface AclPermissions {
  subscribe?: boolean
  publish?: boolean
  retain?: boolean
  id?: string  
}

export declare interface BrokerClientOptions {
  id?: string
  username?: string
  auth?: boolean
  active?: boolean
  intern?: boolean
  will?: any
  timeout?: number
  messageId?: number
  prefix?: string
  readTime?: Date
  writeTime?: Date
  policy?: {[id: string]: ClientPolicy}
  maximumQos?: number
  keepAlive?: number
  sessionTimeout?: number
  version?: number
  socket?: net.Socket
  broker?: Broker
  maxQueueSize?: number
}

export declare interface ClientPolicy {
  clientId?: string | string[]
  subscriptions?: string[] | {[topic: string]: (message: PublishMessage, client: BrokerClient | undefined) => boolean}
  publications?: PublishMessage[]
  permissions?: {[topic: string]: AclPermissions} | TopicCollection<AclPermissions>
  globalPermissions?: {[topic: string]: AclPermissions}
  group?: string
  prefix?: string
  qos?: number
}

/** @description Broker-Client
 */
export class BrokerClient extends EventEmitter {
  id: string = ''
  username: string = ''
  group?: string
  auth: boolean = false
  active: boolean = true
  intern: boolean = false
  will?: TopicItem
  timeout: number = 0
  messageId: number = 1
  prefix: string = ''
  readTime: Date = new Date()
  writeTime: Date = new Date()
  policy?: ClientPolicy
  maximumQos: number = 0
  socket?: net.Socket
  clean?: boolean
  closing?: boolean
  keepAlive?: number
  sessionTimeout?: number
  packetIndex?: number
  broker?: Broker
  maxQueueSize: number = 200

  subscriptions: string[]
  address: string

  constructor (options?: BrokerClientOptions) {
    super()
    if (!options?.id)
      this.id = 'client-' + Math.random().toString(16).slice(2)
    Object.assign(this, options || {})
    this.address = 'intern:' + this.id
    this.subscriptions = []
    this.on('message', (msg: MqttMessage) => {
      this.broker?.addStatistics('messagesSent', 1)
      if (msg.cmd === 'publish' && msg.topic) {
        this.broker?.addStatistics('publishSent', 1)
        this.emit('publish', msg.topic, msg.payload)
        this.emit('topic:' + msg.topic, msg.payload)
      }
    })
  }

  get clientId (): string {
    return this.id
  }

  clone (oldClient: BrokerClient): void {
    this.subscriptions = oldClient.subscriptions
  }  
  close (destroy?: boolean): boolean {
    this.emit('close', destroy)
    return false
  }
}

/** MQTT-Client */
export class MqttBrokerClient extends BrokerClient {
  // @internal
  private _queue: MqttMessage[] = []
  // @internal
  private _queueIndex: number = 0
  // @internal
  private _sending: boolean = false
  // @internal
  private _close?: boolean = false
  // @internal
  private _connecting?: boolean
  // @internal
  private _parser: MqttParser

  qosQueue: {
    inbound: {[messageId: string]: MqttMessage},
    outbound: {[messageId: string]: {client?: {clientId: string, messageId?: number}, messageId?: number, qos?: number}}
  } = {
    inbound: {},
    outbound: {}
  }

  /** New client */
  constructor (options: BrokerClientOptions) {
    super({
      maximumQos: 2,
      ...options
    })
    if (!this.broker)
      throw new Error('Broker is not set')

    const socket: net.Socket | undefined = this.socket
    if (socket) {
      this.address = `${socket.remoteAddress}:${socket.remotePort}`

      if (this.socket) {
        socket.setTimeout(10000)
        socket.on('close', () => this.broker?.closeClient(this))
        socket.on('timeout', () => this.broker?.closeClient(this))
        socket.on('error', () => this.broker?.closeClient(this))
        socket.on('data', (data: Buffer) => {
          this._parse(data)
          this.broker?.addStatistics('bytesReceived', data.length)
        })
      }
    }

    this.on('message', (msg: MqttMessage) => {
      if (!this.socket)
        return
      /** send packet to the client */
      if (this._queue.length >= this.maxQueueSize && msg.topic) {
        if (!this.cleanup(msg.topic))
          this.cleanup(0)
      }
      this._queue.push(msg)
      this._send()
    })

    this._parser = new MqttParser()
    this._parser.on('packet', (msg: MqttMessage) => this.process(msg))
    this._parser.on('error', (err: Error) => {
      const dataError = err.message === 'Invalid input'
      if (!this.closing)
        this.disconnect(dataError ? ERROR_MALFORMED_PACKET : ERROR_UNSPECIFIED)
      if (!dataError)
        this.broker?.emit('error', err)
    })
  }

  /** clone client */
  clone (oldClient: BrokerClient): void {
    this.subscriptions = oldClient.subscriptions
    this.qosQueue = (oldClient as MqttBrokerClient).qosQueue
    this._queue = (oldClient as MqttBrokerClient)._queue
    this.messageId = oldClient.messageId
    this.readTime = new Date()
  }

  /** cleanup sent packets on successfull communication */
  cleanup (cb?: Function | number | string) {
    const queue = this._queue
    const removeItem = (idx: number) => {
      queue.splice(idx, 1)
      if (idx < this._queueIndex)
        this._queueIndex--
      return true
    }

    if (this._queueIndex) {
      if (typeof cb === 'number')
        return removeItem(cb)
      if (typeof cb === 'string') {
        const topic = cb
        cb = function (p: MqttMessage) { return p.cmd === 'publish' && p.topic === topic }
      }
      if (cb) {
        // remove item by filter
        for (let i = 0, n = queue.length; i < n; i++) {
          if (cb(queue[i], i))
            return removeItem(i)
        }
      } else {
        // cleanup old packets
        const oldIdx = this._queueIndex
        let newIdx = 0
        this._queue = queue.filter((packet: MqttMessage, idx) => {
          if (idx >= oldIdx)
            return true
          const cmd = packet.cmd
          if (packet.messageId) {
            if (cmd === 'publish' || cmd === 'pubrec' || cmd === 'pubrel') {
              newIdx++
              return true
            }
            // removeItem(idx, 1)
          }
        })
        this._queueIndex = newIdx
      }
    }
  }

  // @internal
  private _send (): void {
    const msg = this._queue[this._queueIndex]
    if (!msg && this._close)
      this.socket?.end()
    if (!this._sending && msg) {
      this._sending = true

      const fixPayload: Buffer | string = 
        msg.payload === undefined ? '' :
        (msg.payload instanceof Buffer || typeof msg.payload === 'string') ? msg.payload :
        JSON.stringify(msg.payload)
      
      const data: Buffer = this._parser.generate(fixPayload === msg.payload ? msg : {...msg, payload: fixPayload})

      if (!this.socket) {
        this._sending = false
        return
      }

      this.socket.write(data, err => {
        if (!err) {
          this.broker?.addStatistics('bytesSent', data.length)
          this.broker?.addStatistics('messagesSent', 1)
          if (msg.cmd === 'publish' && msg.topic)
            this.broker?.addStatistics('publishSent', 1)
        }
  
        this._sending = false
        if (!err) {
          msg.dup = true
          if (this._queue[this._queueIndex] === msg)
            this._queueIndex++
          this._send()
        }
      })
    }
  }

  /** close client connection */
  close (destroy?: boolean): boolean {
    if (destroy) {
      if (this.qosQueue?.inbound)
        for (const messageId in this.qosQueue.inbound)
          this._removeQosInbound(parseInt(messageId))
      if (this.qosQueue?.outbound)
        for (const messageId in this.qosQueue.outbound)
          this._removeQosOutbound(parseInt(messageId))
    }
    super.close(destroy)
    if (!this.socket)
      return false
    this._close = true
    if (!this._sending && !this._queue[this._queueIndex])
      this.socket.end()
    else if (!this._sending)
      this._send()
    this.socket = undefined
    return true
  }

  // @internal
  private _removeQosInbound (messageId: number) {
    // qosInbound = {messageId: {topic, payload, qos, clients:{clientId:messageId}}, ...}
    const msg = this.qosQueue?.inbound[messageId]
    if (msg) {
      for (const clientId in msg.clients) {
        const destMessageId = msg.clients[clientId]
        let destClient: BrokerClient | undefined = this.broker?.clients.get(clientId)
        if (destClient instanceof MqttBrokerClient) {
          const qosMsg = destClient.qosQueue.outbound[destMessageId]
          if (qosMsg?.client?.clientId === this.clientId)
            delete qosMsg.client
        }
      }
    }
  }

  // @internal
  private _removeQosOutbound (messageId: number, reasonCode?: number) {
    let msgOut = this.qosQueue?.outbound[messageId]
    if (msgOut && msgOut.client && this.qosQueue) {
      delete this.qosQueue.outbound[messageId]
      const origClient: MqttBrokerClient | undefined = this.broker?.clients.get(msgOut.client.clientId) as MqttBrokerClient
      if (origClient?.qosQueue) {
        const origMessageId = msgOut.messageId || 0
        const msgIn = origClient.qosQueue.inbound[origMessageId]
        if (msgIn) {
          delete msgIn.clients?.[this.clientId]
          let hasClients
          if (msgIn.clients)
            for (hasClients in msgIn.clients)
              break
          if (!hasClients && !origClient.closing) {
            this.emit('message', { cmd: msgIn.qos === 2 ? 'pubcomp' : 'puback', messageId: origMessageId, reasonCode })
            delete origClient.qosQueue.inbound[origMessageId]
          }
        }
      }
    }
  }

  /** disconnect client */
  disconnect(reasonCode?: number): void {
    this.emit('message', { cmd: 'disconnect', reasonCode })
    this.broker?.closeClient(this)
  }

  /** process MQTT message */
  async process (msg: MqttMessage): Promise<void> {
    // protocol error: connection request not finisched
    if (this._connecting || !this.broker) {
      this.close()
      return
    }

    this.broker.addStatistics('messagesReceived', 1)

    // cleanup send packets as all they where received
    this.cleanup()
    let granted: number[]
    switch (msg.cmd) {
      case 'connect': {
        if (this.auth)
          return this.disconnect(ERROR_PROTOCOL)
        if (!msg.clientId || this.broker.clients.get(msg.clientId)?.intern)
          return this.disconnect(ERROR_CLIENT_IDENTIFIER)

        this._connecting = true
        this.id = msg.clientId
        this.username = msg.username || ''
        this.will = msg.will || {}
        this.keepAlive = (msg.keepalive || 60) * 1000
        this.sessionTimeout = msg.properties?.sessionExpiryInterval || this.broker.options.sessionTimeout || 60
        this.clean = msg.clean

        await this.broker.auth(this, (msg.password || '').toString('utf8')).catch(() => { })
        const newClient: boolean = this.broker.addClient(this) || true

        if (!this.auth) {
          this.emit('message', { cmd: 'connack', reasonCode: ERROR_BAD_USERNAME_PASSWORD })
          nextTick(() => this.close())
          return
        }
        this.socket?.setTimeout((this.keepAlive || 30000) * 5 / 4)
        this.packetIndex = 0

        this._connecting = false
        this.emit('message', { cmd: 'connack', sessionPresent: !newClient, reasonCode: 0 })
        break
      }
      case 'connack':
      case 'suback':
      case 'unsuback':
      case 'pingresp':
      case 'auth':
        this.disconnect(ERROR_PROTOCOL)
        break
      case 'pingreq':
        this.emit('message', { cmd: 'pingresp' })
        break
      case 'publish':
      case 'pubrel':
        if (msg.cmd === 'pubrel') {
          const messageId = msg.messageId || 0
          const msgIn = this.qosQueue?.inbound[messageId]
          // check for registered inbound message
          if (!msgIn) {
            this.emit('message', { cmd: 'pubcomp', messageId, reasonCode: ERROR_IDENTIFER_NOT_FOUND })
            return
          }
          // inbound message is allready released
          if (!msgIn.topic) {
            this.emit('message', { cmd: 'pubcomp', messageId })
            return
          }
        } else {
          if (msg.payload instanceof Buffer)
            msg.payload = msg.payload.toString('utf8')
          if (typeof msg.payload === 'string' && msg.payload[0] === '{' && msg.payload[msg.payload.length - 1] === '}') {
            try {
              msg.payload = JSON.parse(msg.payload)
            } catch (ignore) { }
          }
          if (typeof msg.payload === 'string' && /^-?\d+(\.\d+)?$/.test(msg.payload))
            msg.payload = parseFloat(msg.payload)
        }
        this.broker.publish(msg, this)
        break
      case 'puback':
        this._removeQosOutbound(msg.messageId || 0)
        break
      case 'pubrec':
        this.cleanup((msg: MqttMessage) => msg.messageId === msg.messageId && msg.cmd === 'publish')
        this.emit('message', { cmd: 'pubrel', messageId: msg.messageId })
        break
      case 'subscribe':
        granted = []
        msg.subscriptions?.forEach(sub => {
          granted.push(this.broker?.subscribe(sub, this) ?? ERROR_UNSPECIFIED)
        })
        this.emit('message', { cmd: 'suback', messageId: msg.messageId, granted: granted })
        break
      case 'unsubscribe':
        granted = []
        msg.unsubscriptions?.forEach(sub => {
          granted.push(this.broker?.unsubscribe(sub, this) ?? ERROR_UNSPECIFIED)
        })
        this.emit('message', { cmd: 'unsuback', messageId: msg.messageId, granted: granted })
        break
      case 'disconnect':
        this.broker?.removeClient(this)
        break
    }
  }

  // @internal
  private _parse(buffer: Buffer): void {
    this._parser.parse(buffer)
  }
}

export declare interface PolicyOptions {
  /** client id list with regex pattern */
  clientId?: string[]
  /** wildcard permissions for topics */
  permissions?: {[topic: string]: AclPermissions}
  /** $clientId|$username */
  prefix?: string
}

export declare interface UserInfo {
  /** policy id */
  policy: string
  /** user group */
  group?: string
  /** user password or validator */
  password?: string | ((usr: string, psw?: string, client?: BrokerClient) => Promise<boolean>)
}

export declare interface BrokerOptions {
  /** broker version string */
  version?: string
  /** connection timeout in seconds */
  connectionTimeout?: number
  /** client session clean timeout in seconds */
  sessionTimeout?: number
  /** keep alive (ping) checking in seconds */
  keepAlive?: number
  /** max queued packets pro connection */
  maxPackets?: number
  /** update statistics time in seconds, default 0 - disabled */
  updateStatistics?: number
  /** server port (plain: 1883, tls: 8883) */
  listen?: string | number
  /** tls options */
  tls?: tls.TlsOptions
  /** set of policy objects */
  policy?: {[id: string]: ClientPolicy} | ((id: string, client?: BrokerClient) => Promise<ClientPolicy> | ClientPolicy)
  /** global topic permissions */
  permissions?: {[topic: string]: AclPermissions}
  /** users */
  users?: ((id: string, psw?: string) => Promise<UserInfo>) | {[id: string]: UserInfo}
  /** log level */
  log?: number
  /** connect handler, used to create MQTTBrokerClient */
  handler?: (socket: net.Socket) => void
}

declare interface BrokerStatistics {
  clientsDisconnected: number
  clientsMax: number
  bytesReceived: number
  bytesSent: number
  messagesReceived: number
  messagesSent: number
  publishReceived: number
  publishSent: number
  publishDropped: number
}

interface BrokerEvents {
  ready: () => void
  log: (message: string) => void
  listen: (address: net.AddressInfo, server?: net.Server) => void
  error: (err: Error, server?: net.Server) => void
  close: () => void
  'clients/connect': (connInfo: { clientId: string, username: string, address: string }) => void
  'clients/disconnect': (connInfo: { clientId: string, username: string, address: string }) => void
  'clients/close': (connInfo: { clientId: string, username: string, address: string }) => void
  publish: (message: PublishMessage, client?: BrokerClient) => void
  [key: `topic:${string}`]: (message: PublishMessage, client?: BrokerClient) => void
}

declare interface SubscribersItem {
  id: string
  qos: number
  nl?: boolean
  rap: boolean
  cb?: (messgae: PublishMessage, client: BrokerClient | undefined) => boolean
}

/** MQTT-Broker server */
export class Broker extends EventEmitter {
  options: BrokerOptions
  startupTime: Date
  permissions: TopicCollection<TopicItem>
  subscribers: TopicCollection<SubscribersItem>
  data: TopicCollection<PublishMessage>
  clients: Map<string, BrokerClient>
  servers: net.Server[] = []
  statistics: BrokerStatistics = {
    clientsDisconnected: 0,
    clientsMax: 0,
    bytesReceived: 0,
    bytesSent: 0,
    messagesReceived: 0,
    messagesSent: 0,
    publishReceived: 0,
    publishSent: 0,
    publishDropped: 0
  }

  // @internal
  private _updateStatistics?: NodeJS.Timeout
  // @internal
  private _expire: WeakMap<BrokerClient, NodeJS.Timeout> = new WeakMap()

  /** Create MQTT server, and start listening */
  constructor (options: BrokerOptions) {
    super()
    this.options = {
      sessionTimeout: 60,
      keepAlive: 300,
      connectionTimeout: 60,
      maxPackets: 200, // max number of packets in list
      updateStatistics: 60,
      listen: '',
      tls: {},
      policy: {}, // {default: {prefix:'$username/$clientId', clientId:['prefix'], permissions:{'#':{subscribe:true, publish:false, retain:false}}}
      permissions: {}, // {'topic': { retain:true }}
      users: {}, // {'user': {policy: 'default', password:'psw'}}
      log: 0,
      ...options
    }

    this.startupTime = new Date()
    this.permissions = new TopicCollection({})
    this.subscribers = new TopicCollection({ systemFilter: true })
    this.data = new TopicCollection({ systemFilter: true })
    this.clients = new Map()

    this.clear()

    this.setOption('permissions', this.options.permissions)
    this.setOption('tls', this.options.tls)

    if (this.options.listen)
      this.listen(this.options)
  }

  /** Start TCP service */
  listen (options?: BrokerOptions) {
    const listen = options?.listen || ''
    if (!listen)
      return
       
    // Start new listeners
    for (const [_, __, proto, ___, host, port] of listen.toString().matchAll(/((\w+):\/\/)?(([^:,]*):)?([^:,]+)/g)) {
      const srv: net.Server = proto === 'tls' || (!proto && options?.tls?.cert)
        ? tls.createServer(options?.tls || this.options.tls || {}, (options?.handler || this.handler).bind(this))
        : net.createServer((options?.handler || this.handler).bind(this))
      const _onerr = (err: Error) => {
        this.emit('error', err, srv)
        srv.close()
      }
      srv.on('error', _onerr)
      srv.listen(parseInt(port), host || '0.0.0.0', () => {
        this.servers.push(srv)
        srv.off('error', _onerr)
        const addr: net.AddressInfo = srv.address() as net.AddressInfo
        if (this.options.log)
          this.emit('log', `MQTT listen on ${addr.address}:${addr.port}`)
        this.emit('listen', addr, srv)
        if (this.servers.length === 1)
          this.emit('ready')
      })
    }
  }

  /** Update option */
  setOption (key: string, value: any) {
    // dynamically change option
    switch (key) {
      case 'listen': {
        this.options.listen = value || '1883'
        this.listen()
        break
      }
      case 'restart': {
        if (value) {
          this.close()
          setTimeout(() => process.exit(1), 1000)
        }
        break
      }
      case 'tls': {
        this.options.tls = value
        if (this.options.tls?.cert && this.options.tls?.key) {
          if (this.options.tls.cert.indexOf('\n') < 0)
            this.options.tls.cert = fs.readFileSync(this.options.tls.cert as string, 'utf8')
          if (this.options.tls.key.indexOf('\n') < 0)
            this.options.tls.key = fs.readFileSync(this.options.tls.key as string, 'utf8')
        } else
          this.options.tls = undefined
        break
      }
      case 'permissions': {
        // remove old permissions
        if (this.permissions.size && this.options.permissions)
          for (let n in this.options.permissions)
            this.permissions.remove(n)
        this.permissions.set('#', { publish: false })
        this.permissions.set('$SYS/#', { publish: false, retain: true })
        this.permissions.set('$share/+', { publish: false })
        if (typeof value === 'function')
          value = value.call(this)
        value = typeof value === 'object' ? value : {}
        this.options.permissions = { ...value }
        for (let n in value)
          this.permissions.set(n, value[n])
        break
      }
      default:
        if (key in this.options)
          (this.options as any)[key] = value
        break
    }
  }

  /** Close server */
  async close () {
    if (this.options.log)
      this.emit('log', 'Closing')
    this.clear()
    clearTimeout(this._updateStatistics)
    this._updateStatistics = undefined
    for (const srv of this.servers)
      await new Promise(resolve => srv.close(resolve))
    this.servers.splice(0)
    this.emit('close')
  }

  /** Update broker statistics */
  updateStatistics () {
    const stat = this.statistics
    if (!stat)
      return

    if (stat.clientsMax < this.clients.size)
      stat.clientsMax = this.clients.size

    const data: {[topic: string]: string | number} = {
      'version': this.options.version || BROKER_VERSION,
      'clients/connected': this.clients.size - stat.clientsDisconnected,
      'clients/disconnected': stat.clientsDisconnected,
      'clients/total': this.clients.size,
      'clients/maximum': stat.clientsMax,
      'load/bytes/received': stat.bytesReceived,
      'load/bytes/sent': stat.bytesSent,
      'messages/received': stat.messagesReceived,
      'messages/sent': stat.messagesSent,
      'messages/publish/received': stat.publishReceived,
      'messages/publish/sent': stat.publishSent,
      'messages/publish/dropped': stat.publishDropped,
      'messages/retained/count': this.data.size,
      'subscriptions/count': this.subscribers.size,
      'time': new Date().getTime() / 1000 | 0,
      'uptime': (new Date().getTime() - this.startupTime.getTime()) / 1000 | 0
    }

    for (const n in data)
      this.publish({ topic: '$SYS/broker/' + n, payload: data[n], retain: true })

    clearTimeout(this._updateStatistics)
    this._updateStatistics = undefined
    const timeout: number = this.options.updateStatistics || 0
    if (timeout)
      this._updateStatistics = setTimeout(() => this.updateStatistics(), timeout * 1000)
  }

  /** Clear all active connections,subscriptions,publications */
  clear () {
    this.subscribers.clear()
    this.data.clear()
    for (const [id, client] of this.clients.entries()) {
      if (client.socket)
        client.socket.destroy()
    }
    this.clients.clear()

    this.statistics = {
      clientsDisconnected: 0,
      clientsMax: 0,
      bytesReceived: 0,
      bytesSent: 0,
      messagesReceived: 0,
      messagesSent: 0,
      publishReceived: 0,
      publishSent: 0,
      publishDropped: 0
    }
    this.updateStatistics()
  }

  /** Get client by id */
  getClient(id: string): BrokerClient | undefined {
    return this.clients.get(id)
  }

  /** Add client to the broker */
  addClient (client: BrokerClient): boolean {
    if (client instanceof MqttBrokerClient) {
      if (this.options.log)
        this.emit('log', `CONNECT ${client.id} ${client.address} auth:${!!client.auth}`)
      if (!client.auth)
        return false
    }

    let oldClient = this.clients.get(client.id)
    if (oldClient) {
      clearTimeout(this._expire.get(oldClient))
      this._expire.delete(oldClient)
    }

    if (client.clean && oldClient) {
      this.removeClient(oldClient)
      oldClient = undefined
    }
    delete client.clean
    this.clients.set(client.id, client)
    if (oldClient) {
      client.clone(oldClient)
      if (client instanceof MqttBrokerClient)
        return false
    }

    if (!client.policy)
      client.policy = {}
    if (!client.group && client.policy.group)
      client.group = client.policy.group
    if (!client.prefix && client.policy.prefix)
      client.prefix = (client.policy.prefix.replace(/\$(\S+)/g, (name: string) => this._subOption(client, name) || '') + '/').replace(/^\/+/, '')
    if (client.prefix && !client.prefix.endsWith('/'))
      client.prefix += '/'

    if (client.policy.globalPermissions)
      Object.entries(client.policy.globalPermissions).forEach(([n, v]) => {
        if (n.startsWith('/'))
          n.substring(1)
        else
          n = client.prefix + n
        this.permissions.add(n, { ...v, id: client.id })
      })

    if (!client.policy.permissions)
      client.policy.permissions = {}
    if (client.policy?.permissions && !(client.policy.permissions instanceof TopicCollection)) {
      const col = new TopicCollection<AclPermissions>({ systemFilter: true })
      if (client.prefix)
        col.add(client.prefix + '#', { publish: true, subscribe: true })
      for (let n in client.policy.permissions) {
        const perm = client.policy.permissions[n]
        if (n.startsWith('/'))
          n = n.substring(1)
        else if (client.prefix)
          n = client.prefix + n
        if (n.startsWith('$SYS/'))
          delete perm.publish // ignore $SYS publish permissions
        col.add(n, perm)
      }
      client.policy.permissions = col
    }

    if (client.policy?.subscriptions) {
      if (Array.isArray(client.policy.subscriptions))
        client.policy.subscriptions.forEach((sub: string) => this.subscribe(sub, client))
      else
        Object.entries(client.policy.subscriptions)
          .forEach(([sub, v]) => this.subscribe(sub, client, typeof v === 'function' ? v : undefined))
    }
    /*if (client.subscriptions) {
      if (Array.isArray(client.subscriptions))
        client.subscriptions.forEach((sub: string) => this.subscribe(sub, client))
      else
        Object.entries(client.subscriptions)
          .forEach(([sub, v]) => this.subscribe(sub, client, typeof v === 'function' ? v : undefined))
    }*/

    if (client.policy?.publications)
      client.policy.publications.forEach((pub: PublishMessage) => this.publish(pub, client))

    //client.keepAlive = this.connectionTimeout / 2
    this.emit('clients/connect', { clientId: client.clientId, username: client.username, address: client.address })
    return true
  }

  /** Remove client from the broker */
  removeClient (client: BrokerClient) {
    const closeOk: boolean = client.close()
    if (client.auth && this.clients.get(client.id) === client && closeOk) {
      if (this.options.log)
        this.emit('log', `DISCONNECT ${client.id}`)
      client.closing = true
      this.closeClient(client)
      this.emit('clients/disconnect', { clientId: client.clientId, username: client.username, address: client.address })
    }
    client.close(true)
    client.subscriptions.forEach(topic => this.unsubscribe(topic, client))
    if (client.policy?.globalPermissions) {
      for (const n in client.policy.globalPermissions) {
        const topic = n.startsWith('/') ? n.substring(1) : (client.prefix + n)
        this.permissions.remove(topic, client.id)
      }
    }
    clearTimeout(this._expire.get(client))
    this._expire.delete(client)
    if (this.clients.get(client.id) === client) {
      this.clients.delete(client.id)
      if (!client.active && this.statistics)
        this.statistics.clientsDisconnected--
    }
  }

  /** If client sends disconnect message */
  disconnect (client: BrokerClient) {
    if (client.auth && this.clients.get(client.id) === client) {
      if (this.options.log)
        this.emit('log', `DISCONNECT ${client.id}`)
      client.closing = true
      this.closeClient(client)
      this.removeClient(client)
      this.emit('clients/disconnect', { clientId: client.clientId, username: client.username, address: client.address })
    } else
      client.close()
  }

  /** Publish topic=payload */
  set (topic: string, payload: string | Buffer, retain?: boolean, qos?: number) {
    this.publish({
      topic: topic,
      payload: payload,
      retain: retain === undefined ? false : retain,
      qos: qos || 0
    })
  }

  /** Get retain messages */
  get (topic: string): PublishMessage | undefined {
    return this.data.get(topic)
  }

  /** Update policy */
  setPolicy (policy: { [id: string]: PolicyOptions }) {
    this.options.policy = { ...policy }
    if (!this.options.policy.default)
      this.options.policy.default = { prefix: '$username/$clientId' }
    if (!this.options.policy.admin)
      this.options.policy.admin = { prefix: '', permissions: { '#': { subscribe: true, publish: true } } }
  }

  /** Update users set */
  setUsers (users: ((id: string, psw?: string) => Promise<UserInfo>) | {[id: string]: UserInfo}) {
    this.options.users = users
  }

  /** Authorize user */
  async auth (client: BrokerClient, password: string): Promise<BrokerClient> {
    let user: UserInfo | undefined
    let policy: ClientPolicy | undefined

    if (typeof this.options.users === 'function')
      user = await this.options.users(client.username)
    else
      user = this.options.users?.[client.username]
    if (user) {
      if (typeof this.options.policy === 'function')
        policy = await this.options.policy(user.policy, client)
      else
        policy = this.options.policy?.[user.policy] || this.options.policy?.default
      if (policy && (policy as ClientPolicy).clientId) {
        let arr: string[] | string | undefined = (policy as ClientPolicy).clientId
        if ((typeof arr === 'string' && !client.clientId.match(arr)) ||
          (Array.isArray(arr) && !arr.find((v: string) => client.clientId.match(v))))
          throw new Error('Access denied')
      }

      if (!client.auth && password) {
        if (policy && typeof user.password === 'function')
          client.auth = await (user.password?.(client.username, password, client))
        else
          client.auth = password === user.password
      }
      if (client.auth && policy) {
        client.policy = { ...policy }
        client.group = user.group || policy.group
        client.prefix = policy.prefix
          ? (policy.prefix.replace(/\$(\S+)/g, (_: string, name: string) => this._subOption(user, name) || this._subOption(client, name) || '') + '/').replace(/^\/+/, '') : ''
      }
    }
    if (!client.auth)
      throw new Error('Access denied')
    return client
  }

  /** Add statistics value */
  addStatistics (id: keyof BrokerStatistics, value: number) {
    if (id in this.statistics)
      this.statistics[id] += value
  }

  // @internal
  private _subOption (obj: any, name: string): string {
    let option = obj
    for (const n of name.matchAll(/[^.]+/g))
      option = typeof option === 'object' ? option[n[0]] : undefined
    return option !== undefined ? option.toString() : ''
  }

  /** close client connection */
  closeClient (client: BrokerClient) {
    if (client.close() && client.auth) {
      const timeout: number = client.sessionTimeout || 0
      if (timeout)
        this._expire.set(client, setTimeout(() => {
          this._expire.delete(client)
          this.removeClient(client)
        }, timeout * 1000))

      client.active = false
      if (this.clients.get(client.id) === client)
        this.statistics.clientsDisconnected++
      if (this.options.log)
        this.emit('log', `CLOSE ${client.id}`)

      if (client.will?.topic)
        this.publish(client.will, client)

      this.emit('clients/close', { clientId: client.clientId, username: client.username, address: client.address })

      // remove client if no subscriptions
      if (client.subscriptions.length === 0 || client.sessionTimeout === 0)
        this.removeClient(client)
    }
  }

  /** fix client topic */
  // @internal
  private _fixTopic (client: BrokerClient, topic: string, permissionId?: string, defaultPermission?: boolean): { topic: string, reasonCode: number } {
    let reasonCode = client ? ERROR_NOT_AUTHORIZED : 0
    if (client && topic) {
      let hasPrefix = false
      if (topic.startsWith('/'))
        topic = topic.substring(1)
      else
        if (client.prefix && !topic.startsWith('$')) {
          topic = client.prefix + topic
          hasPrefix = true
        }

      reasonCode =
        (!topic.startsWith('$') ||
          topic.startsWith('$SYS/') ||
          topic.startsWith('$share/') ||
          topic.indexOf('//') < 0) ? 0 : ERROR_TOPIC_INVALID
      if (!reasonCode && permissionId)
        reasonCode = this._permission(client, topic, permissionId, defaultPermission) ? 0 : ERROR_TOPIC_INVALID
    }
    return {
      topic,
      reasonCode
    }
  }

  /** Process incomming publish messages */
  process (msg: PublishMessage, intern?: boolean): boolean | undefined {
    if (intern)
      return true
    if (msg.retain) {
      const topic = msg.topic || ''
      let data = this.data.get(topic)
      if (!data)
        data = this.data.set(topic, { topic, retain: true, qos: 0 })
      if (data.payload === msg.payload)
        return false
      data.payload = msg.payload
    }
  }

  /** Publish message */
  publish (msg: PublishMessage, client?: BrokerClient) {
    let topic: string = msg.topic as string, reasonCode = 0, retain = msg.retain

    // if client is intern, can publish qos>0, receive only qos=0
    msg.qos = msg.qos || 0

    if (client && !(msg as MqttMessage).clients) {
      this.statistics.publishReceived++

      ({ topic, reasonCode } = this._fixTopic(client, topic, 'publish', true))
      if (!reasonCode)
        retain = this._permission(client, topic, 'retain', retain ?? false)
      if (msg.qos === 2 && client.maximumQos === 2) {
        if (client instanceof MqttBrokerClient) {
          if (!reasonCode && client.qosQueue.inbound[msg.messageId || 0])
            reasonCode = ERROR_MESSAGEID_INUSE
          if (!reasonCode)
            client.qosQueue.inbound[msg.messageId || 0] = { topic: topic, payload: msg.payload, qos: msg.qos, retain, messageId: msg.messageId || 0, clients: {} }
        }
        if (reasonCode)
          this.statistics.publishDropped++
        client.emit('message', { cmd: 'pubrec', messageId: msg.messageId, reasonCode })
        return
      }
    }

    function payloadString(packet: MqttMessage) {
      if (typeof packet.payload !== 'string' && !(packet.payload instanceof Buffer))
        return JSON.stringify(packet.payload)
      return packet.payload.toString()
    }

    if (!client) {
      if ((this.options.log || 0) > 1 && !topic.startsWith('$SYS/'))
        this.emit('log', `PUBLISH topic:${topic} payload:${payloadString(msg)} retain:${retain}`)
    } else {
      if ((this.options.log || 0) > 1)
        this.emit('log',`PUBLISH topic:${topic} payload:${payloadString(msg)} reason:${reasonCode} retain:${retain} qos:${msg.qos} from:${client.id}`)
    }

    //if (client && reasonCode && packet.qos === 1)
    //  client.send({ cmd: 'puback', messageId: packet.messageId, reasonCode })
    //if (reasonCode)
    //  return

    let qosClients: { [clientId: string]: number } | undefined
    const processMessage = { topic: topic, payload: msg.payload, qos: msg.qos || 0, retain }
    if (reasonCode)
      this.statistics.publishDropped++
    if (!reasonCode && this.process(processMessage, msg.intern) !== false) {
      this.emit('publish', processMessage, client)
      this.emit('topic:' + topic, processMessage, client)

      let pubList: {[id: string]: MqttMessage} = {}
      this.subscribers.iterate(topic, (sub: SubscribersItem) => {
        const subClient = this.clients.get(sub.id)
        const qos = Math.min(sub.qos, processMessage.qos, subClient?.maximumQos || 0)
        if (subClient && !subClient.closing && (subClient.active || qos > 0) && !pubList[sub.id] && (!sub.nl || (client && client.id !== sub.id))) {
          const hasPrefix = subClient.prefix && topic.startsWith(subClient.prefix)
          if (hasPrefix || this._permission(subClient, topic, 'subscribe', true)) { // get all prefixed messages by policy, check acl read permission
            let subtopic = processMessage.topic
            if (hasPrefix)
              subtopic = subtopic.substring(subClient.prefix.length)
            const message = { cmd: 'publish', qos, reference: processMessage.topic, topic: subtopic, payload: processMessage.payload, retain: sub.rap ? msg.retain : false }
            if (sub.cb && sub.cb(message, client) === false) {
              pubList = {}
              return false
            } else {
              pubList[sub.id] = message
            }
          }
        }
        return false
      })
      Object.entries(pubList).forEach(([id, message]) => {
        if ((this.options.log || 0) > 2)
          this.emit('log', `> ${id} topic:${message.topic} payload:${payloadString(message)}`)
        const subClient = this.clients.get(id)
        if (!(subClient instanceof MqttBrokerClient))
          return
        subClient.emit('message', message)
        if (message.messageId && client?.maximumQos) {
          subClient.qosQueue.outbound[message.messageId || 0] = { qos: message.qos, client: client && { clientId: client.clientId, messageId: msg.messageId } }
          if (!qosClients)
            qosClients = {}
          qosClients[subClient.clientId] = message.messageId
        }
      })
    }

    if (client instanceof MqttBrokerClient && client.maximumQos && msg.qos) {
      if (qosClients) {
        client.qosQueue.inbound[msg.messageId || 0] = { qos: msg.qos, clients: qosClients }
      } else {
        delete client.qosQueue.inbound[msg.messageId || 0]
        client.emit('message', { cmd: msg.qos === 2 ? 'pubcomp' : 'puback', messageId: msg.messageId, reasonCode })
      }
    }
  }

  /** subscribe single client topic */
  subscribe (sub: { topic: string, qos?: number, rh?: number, nl?: boolean, rap?: boolean } | string, client: BrokerClient, cb?: (message: MqttMessage, client: BrokerClient | undefined) => boolean): number {
    if (typeof sub === 'string')
      sub = { topic: sub }
    if (client.subscriptions.includes((sub as any).topic))
      return sub.qos || 0
    const { topic, reasonCode } = this._fixTopic(client, sub.topic, 'subscribe', true)
    if (reasonCode)
      return reasonCode
    sub.qos = Math.min(client.policy?.qos !== undefined ? client.policy.qos : 2, sub.qos || 0)

    if ((this.options.log || 0) > 1)
      this.emit('log', `SUB ${client.id} topic:${topic} qos:${sub.qos} rh:${sub.rh||0}`)

    // resend retain messages
    if (sub.rh !== 2) {
      let process = true
      if (sub.rh === 1)
        this.subscribers.iterate(topic, () => {
          process = false
          return false
        })

      if (process)
        this.data.iterateWildcard(topic, (pub: MqttMessage) => {
          if (pub.retain) {
            const hasPrefix = client.prefix && topic.startsWith(client.prefix)
            if (hasPrefix || this._permission(client, topic, 'subscribe', true)) { // get all prefixed messages by policy, check acl read permission
              let subtopic: string = pub.topic as string
              if (hasPrefix)
                subtopic = subtopic.substring(client.prefix.length)

              const message = { cmd: 'publish', qos: Math.min(pub.qos || 0, sub.qos || 0), reference: pub.topic, topic: subtopic, payload: pub.payload, retain: sub.rap || false }
              // retained messages are sent without client
              if (!cb || cb(message, undefined) !== false) {
                if ((this.options.log || 0) > 2)
                  this.emit('log', '> ' + client.id + ' topic:' + message.topic + ' payload:' + message.payload)
                client.emit('message', message)
              }
            }
          }
          return false
        })
    }

    this.subscribers.add(topic, { id: client.id, qos: sub.qos, nl: sub.nl, rap: !!sub.rap, cb })
    client.subscriptions.push(sub.topic)
    return sub.qos
  }

  /** unsubscribe single client topic */
  unsubscribe (topic: string, client: BrokerClient): number {
    const fixTopic = this._fixTopic(client, topic)
    if ((this.options.log || 0) > 1)
      this.emit('log', `UNSUB ${client.id} topic:${fixTopic.topic}`)

    if (!fixTopic.reasonCode && this.subscribers.remove(fixTopic.topic, client.clientId)) {
      const i = client.subscriptions.indexOf(topic)
      if (i >= 0)
        client.subscriptions.splice(i, 1)
      return 0
    }
    return fixTopic.reasonCode || 0x11
  }

  /** get permissions value */
  // @internal
  private _permission (client: BrokerClient, topic: string, permissionId: string, defaultPermission: boolean = true) {
    //defaultPermission = typeof defaultPermission === 'undefined' ? true : defaultPermission
    const acl = this.permissions.getOption(topic, permissionId, defaultPermission)
    return client?.policy?.permissions instanceof TopicCollection ? client.policy.permissions.getOption(topic, permissionId, acl) : acl
  }

  /** MQTT connection handler to create MqttBrokerClient */
  handler (socket: net.Socket) {
    new MqttBrokerClient({broker: this, socket})
  }

  /** Add one time listener or call immediatelly for 'ready' */
  once<K extends keyof BrokerEvents>(event: K, listener: BrokerEvents[K]): this {
    if (event === 'ready' && this.servers.length)
      (listener as BrokerEvents['ready'])()
    else
      super.once(event, listener)
    return this
  }

  on<K extends keyof BrokerEvents>(event: K, listener: BrokerEvents[K]): this {
    if (event === 'ready' && this.servers.length)
      (listener as BrokerEvents['ready'])()
    super.on(event, listener)
    return this
  }
  // @ts-ignore
  addListener<K extends keyof BrokerEvents>(event: K, listener: BrokerEvents[K]): this;// { return super.addListener(event, listener) }
  // @ts-ignore
  off<K extends keyof BrokerEvents>(event: K, listener: BrokerEvents[K] ): this; // { return super.off(event, listener) }
  // @ts-ignore
  removeListener<K extends keyof BrokerEvents>(event: K, listener: BrokerEvents[K]): this; // { return super.removeListener(event, listener) }
}

export interface MqttConnectionOptions {
  url: string
  clientId?: string
  auth?: string
  username?: string
  password?: string
  protocolVersion?: 4 | 5
  tlsFingerprint?: string
}

const MqttConnectionState = {
  connecting: 0,
  connected: 1,
  closing: 2,
  closed: 3,
  disconnecting: 4,
  disconnected: 5,
} as const;

type MqttConnectionState = typeof MqttConnectionState[keyof typeof MqttConnectionState];

interface MqttConnectionEvents {
  connect: () => void
  close: () => void
  disconnect: (reasonCode: number) => void
  publish: (message: MqttMessage) => void
  error: (err: Error) => void
  [key: `topic:${string}`]: (payload: string | Buffer, retain?: boolean) => void
}

declare interface MqttConnectionSubscription {
  qos: number;
  cb?: (messgae: MqttMessage) => void;
}

/** MQTT connection */
export class MqttConnection extends EventEmitter {
  public options: MqttConnectionOptions
  // @internal
  private _state: MqttConnectionState = MqttConnectionState.disconnected
  // @internal
  private _socket: net.Socket | undefined
  // @internal
  private _parser: MqttParser
  // @internal
  private _subscribtions: TopicCollection<MqttConnectionSubscription> = new TopicCollection({})
  // @internal
  private _queue: MqttMessage[] = []
  // @internal
  private _messageId = 1
  // @internal
  private _timer?: NodeJS.Timeout
  // @internal
  private _keepAlive = 60
  // @internal
  private _reconnectTimeout = 5000
  // @internal
  private _cleanSession = true

  constructor(options: MqttConnectionOptions) {
    super()
    this.options = options
    this._parser = new MqttParser(options.protocolVersion || 4)
    this._parser.on('packet', (message: MqttMessage) => {
      if (!this._socket) return
      switch (message.cmd) {
        case 'connack':
          if (this._state === MqttConnectionState.connecting) {
            this._reconnectTimeout = 5000
            if (message.returnCode === 0) {
              this._state = MqttConnectionState.connected
              this.emit('connect')
              const subscriptions = Object.entries(this._subscribtions.all).map(([topic, item]) => ({ topic, qos: item.items?.[0].qos || 0 }))
              if (subscriptions.length)
                this._send({ cmd: 'subscribe', messageId: this._messageId++, subscriptions })
              for (const msg of this._queue)
                this._send(msg)
              this._ping()
            } else {
              this._cleanSession = true
              this.emit('disconnect', message.returnCode!)
              this._socket?.destroy()
            }
          } else {            
            this.emit('error', new Error('Invalid message: ' + message.cmd))
            this._state = MqttConnectionState.closing
            this._socket?.destroy()
          }
          break
        case 'publish':
          this._cleanSession = false
          if (message.qos === 1)
            this._send({ cmd: 'puback', messageId: message.messageId })
          if (message.qos === 2)
            this._send({ cmd: 'pubrec', messageId: message.messageId })

          this._subscribtions.iterate(message.topic!, (sub: MqttConnectionSubscription) => (sub.cb?.(message), true))
          this.emit('publish', message)
          this.emit(`topic:${message.topic}`, message.payload as string | Buffer, message.retain)
          break
        case 'puback': {
          const idx = this._queue.findIndex(m => m.messageId === message.messageId)
          if (idx !== -1)
            this._queue.splice(idx, 1)
          break
        }
        case 'pubrec':
          const idx = this._queue.findIndex(m => m.messageId === message.messageId)
          if (idx !== -1)
            this._queue.splice(idx, 1)
          this._send({ cmd: 'pubrel', messageId: message.messageId })
          break
        case 'pubcomp':
          break
        case 'pubrel':
          this._send({ cmd: 'pubcomp', messageId: message.messageId })
          break
        case 'suback':
        case 'unsuback':
          break
        case 'disconnect':
          this.emit('disconnect', message.returnCode!)
          this._socket?.destroy()
          break
        case 'pingresp':
          this._queue = this._queue.filter(m => m.qos)
          break
        default:
          this.emit('error', new Error('Invalid message: ' + message.cmd))
          this._state = MqttConnectionState.closing
          this._socket?.destroy()
          break
      }
    })
  }

  /** Connect to the MQTT broker */
  connect(connectMsg?: ConnectMessage) {
    this._state = MqttConnectionState.connecting
    const url = new URL(this.options.url)
    const connOptions: tls.ConnectionOptions = {
      host: url.hostname,
      port: parseInt(url.port) || (url.protocol === 'mqtts:' ? 8883 : 1883),
      rejectUnauthorized: false,
      timeout: 10000
    }
    this._socket = url.protocol === 'mqtts:' ? tls.connect(connOptions) : net.connect(connOptions as net.NetConnectOpts, () => {
      const {username, password, clientId, protocolVersion} = this.options
      this._send({
        cmd: 'connect',
        clientId: clientId || username || 'node-mqtt-' + Date.now().toString(36).slice(2),
        username,
        password,
        protocolVersion,
        clean: this._cleanSession,
        keepalive: this._keepAlive,
        ...connectMsg
      })
    })
    this._socket.on('secureConnect', () => {
      const fingerprint = (this._socket as tls.TLSSocket).getPeerCertificate().fingerprint
      if (this.options.tlsFingerprint && fingerprint !== this.options.tlsFingerprint) {
        this._socket?.destroy()
        return
      }
    })
    this._socket.on('error', () => {})
    this._socket.on('data', (data: Buffer) => {
      try {
        this._parser.parse(data)
      } catch (err: any) {
        this.emit('error', err)
        this._socket?.destroy()
      }
    })
    this._socket.on('close', () => {
      clearTimeout(this._timer)
      this._timer = undefined
      this._socket = undefined
      if (this._state === MqttConnectionState.connecting)
        this._cleanSession = true
      if (this._state === MqttConnectionState.disconnecting) {
        this._state = MqttConnectionState.disconnected
        this._cleanSession = true
      } else {
        this._state = MqttConnectionState.closed
        this._timer = setTimeout(() => this.connect(), this._reconnectTimeout)
        if (this._reconnectTimeout < 300000)
          this._reconnectTimeout *= 2
        else
          this._reconnectTimeout = 300000
      }
      this.emit('close')
    })
  }

  /** Get connection state */
  get state(): MqttConnectionState {
    return this._state
  }

  // @internal
  private _ping() {
    clearTimeout(this._timer)
    this._timer = setTimeout(() => {
      this._send({ cmd: 'pingreq' })
      this._ping()    
    }, this._keepAlive * 1000)
  }

  /** Send publish message */
  publish(topic: string, payload: string, qos = 0, retain = false) {
    const msg: MqttMessage = { topic, payload, qos, retain, messageId: this._messageId++ }
    this._queue.push(msg)
    if (this._state === MqttConnectionState.connected) {
      this._ping()
      this._send(msg)
    }
    else if (retain) {
      const idx = this._queue.findIndex(m => m.topic === topic && m.retain)
      if (idx !== -1)
        this._queue.splice(idx, 1)
    }
    if (this._queue.length > 100) {
      let idx = this._queue.findIndex(m => !m.qos && !m.retain)
      if (idx !== -1)
        this._queue.splice(idx, 1)
      else {
        idx = this._queue.findIndex(m => !m.qos)
        if (idx !== -1)
          this._queue.splice(idx, 1)
      }
    }
  }

  /** Send subscribe */
  subscribe(topic: string, qos?: 0 | 1 | 2, listener?: ( message: MqttMessage) => void) {
    if (this._subscribtions.all[topic]?.items?.length)
      throw new Error('Already subscribed to topic: ' + topic)
    this._subscribtions.add(topic, { qos: qos || 0, cb: listener })
    if (this._state === MqttConnectionState.connected) {
      this._ping()
      this._send({ cmd: 'subscribe', messageId: this._messageId++, subscriptions: [{ topic, qos: qos || 0 }] })
    }
  }

  /** Send unsubscribe */
  unsubscribe(topic: string) {
    if (!this._subscribtions.all[topic]?.items?.length)
      return
    this._subscribtions.remove(topic)
    if (this._state === MqttConnectionState.connected) {
      this._send({ cmd: 'unsubscribe', messageId: this._messageId++, subscriptions: [{ topic }] })
    }
  }

  /** Send disconnect message and close connection */
  disconnect(reasonCode = 0) {
    clearTimeout(this._timer)
    this._timer = undefined
    this._reconnectTimeout = 5000
    if (this._socket) {
      this._state = MqttConnectionState.disconnecting
      this._send({ cmd: 'disconnect', reasonCode })
      this._socket?.end()
      this.emit('disconnect', reasonCode)
    } else {
      this._state = MqttConnectionState.disconnected
    }
  }

  // @internal
  private _send(msg: MqttMessage) {
    this._socket?.write(this._parser.generate(msg))
  }

  // @ts-ignore
  on<K extends keyof MqttConnectionEvents>(event: K, listener: MqttConnectionEvents[K]): this;// { super.on(event, listener);return this}
  // @ts-ignore
  addListener<K extends keyof MqttConnectionEvents>(event: K, listener: MqttConnectionEvents[K]): this;// { return super.addListener(event, listener) }
  // @ts-ignore
  once<K extends keyof MqttConnectionEvents>(event: K, listener: MqttConnectionEvents[K]): this; // {super.once(event, listener);return this}
  // @ts-ignore
  off<K extends keyof MqttConnectionEvents>(event: K, listener: MqttConnectionEvents[K] ): this; // { return super.off(event, listener) }
  // @ts-ignore
  removeListener<K extends keyof MqttConnectionEvents>(event: K, listener: MqttConnectionEvents[K]): this; // { return super.removeListener(event, listener) }
}
