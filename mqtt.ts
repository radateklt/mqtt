import {EventEmitter} from 'events'
import net from 'net'
import tls from 'tls'
import fs from 'fs'

const BROKER_VERSION = 'MQTT-Broker'

export enum MQTT_ERROR {
  UNSPECIFIED = 0x80,
  MALFORMED_PACKET = 0x81,
  PROTOCOL = 0x82,
  CLIENT_IDENTIFIER = 0x85,
  BAD_USERNAME_PASSWORD = 0x86,
  NOT_AUTHORIZED = 0x87,
  MESSAGEID_INUSE = 0x91,
  IDENTIFER_NOT_FOUND = 0x92
}

/*
supported MQTT features:
  MQTT 3.1.1, 5.0
  connect: username, password, will, clean, keepalive, sessionExpiryInterval
  publish: qos=0-2, retain
  puback:
  subscribe: qos=0-2, rh (Retain Handling), nl (No Local), rap (Retain as Published)
  unsubscribe:
  policy: permissions, topic prefix, init subscriptions, init publications
*/

declare interface MqttMessage {
  protocolVersion?: number

  cmd?: string

  protocol?: string         // connect
  clientId?: string         // connect
  will?: {                  // connect
    topic?: string,
    payload?: string | Buffer | number,
    qos?: number,
    retain?: boolean,
    properties?: MqttProperties
  }
  username?: string         // connect
  password?: string | Buffer// connect
  clean?: boolean           // connect
  sessionPresent?: boolean  // connect
  keepalive?: number        // connect

  topic?: string            // publish
  payload?: any             // publish
  dup?: boolean             // publish
  qos?: number              // publish
  retain?: boolean          // publish

  subscriptions?: Array<string | MqttSubscription> // subscribe
  unsubscriptions?: string[] // unsubscribe
  granted?: number[]        // suback, unsuback

  messageId?: number        // publish, puback, pubrec, pubrel, pubcomp, suback
  reasonCode?: number       // connack, puback, pubrec, pubrel, pubcomp, suback, disconnect
  returnCode?: number       // connack v3.1.1

  properties?: MqttProperties

  intern?: boolean          // intern
  clients?: {[clientId: string]: number} // intern
}

declare interface MqttSubscription {
  topic: string
  qos?: number
  rh?: number
  nl?: boolean
  rap?: boolean
}

declare interface MqttProperties {
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
        case MQTT_ERROR.UNSPECIFIED:
          break
        case MQTT_ERROR.PROTOCOL:
          reasonCode = 1
          break
        case MQTT_ERROR.CLIENT_IDENTIFIER:
          reasonCode = 2
          break
        case MQTT_ERROR.BAD_USERNAME_PASSWORD:
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
  private list: Buffer[] = []
  private size: number = 0
  private lastSize: number = 0
  private lastHdr: number = 0

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
    if (this.lastSize < 0)
      return false
    this.list.push(data)
    this.size += data.length

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
    this.list =[]
    this.size = 0
    this.lastSize = 0
    this.lastHdr = 0
  }

  private _hasPacket(): boolean {
    let buf: Buffer
    let size: number = this.lastSize
    let bits: number = 0
    if (size)
      return size <= this.size
    for (let top = 0, idx = 1, hdr = 2; buf = this.list[top]; top++, idx = 0) {
      for (let n = buf.length; idx < n; idx++, hdr++) {
        const b = buf[idx]
        size = size | ((b & 0x7F) << bits)
        bits += 7
        if (!(b & 0x80)) {
          size += hdr
          if (hdr > 5) {
            this.lastSize = -1
            throw new Error('Invalid input')
          }
          this.lastHdr = hdr
          this.lastSize = size
          return size <= this.size
        }
      }
    }
    return false
  }

  private _getPacket(): MqttMessage | undefined {
    if (this._hasPacket()) {
      let size: number = this.lastSize, top: number = 0
      for (; size > 0; top++)
        size -= this.list[top].length

      if (top > 0) {
        const buf: Buffer = Buffer.concat(this.list.splice(0, top + 1, EMPTY_BUFFER))
        this.list[0] = buf
      }

      const header: number = this.list[0][0]
      const payload: Buffer = this.list[0].subarray(this.lastHdr, this.lastSize)
      this.list[0] = this.list[0].subarray(this.lastSize)
      this.size -= this.lastSize
      this.lastSize = 0
      if (!this.list[0].length)
        this.list.shift()

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
  read?: boolean
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
  subscriptions?: string[] | {[topic: string]: (message: MqttMessage) => boolean}
  publications?: MqttMessage[]
  permissions?: {[topic: string]: AclPermissions} | TopicCollection<AclPermissions>
  globalPermissions?: {[topic: string]: AclPermissions}
  group?: string
  prefix?: string
  qos?: number
}

/** @description Broker-Client
 */
export class BrokerClient {
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
  _connecting?: boolean
  broker?: Broker
  maxQueueSize: number = 200

  subscriptions: string[]
  protected _address?: string

  constructor (options?: BrokerClientOptions) {
    if (!options?.id)
      this.id = 'client-' + Math.random().toString(16).slice(2)
    Object.assign(this, options || {})
    this.subscriptions = []
  }

  get clientId (): string {
    return this.id
  }

  get address () {
    return this._address
  }
  
  clone (oldClient: BrokerClient): void {}  
  cleanup(cb?: Function): void {}
  write (msg: MqttMessage, data: Buffer, cb: (err?: Error) => void): void {}
  publish (topic: string, payload: any): void {}
  send (msg: MqttMessage): void {
    this.broker?.addStatistics('messagesSent', 1)
    if (msg.cmd === 'publish' && msg.topic) {
      this.broker?.addStatistics('publishSent', 1)
      this.publish(msg.topic, msg.payload)
    }
  }
  
  close (destroy?: boolean): boolean { return false}
}

/** @description MQTT-Client
 */
export class MqttClient extends BrokerClient {
  protected _queue: MqttMessage[] = []
  protected _queueIndex: number = 0
  _sending: boolean = false
  protected parser: MqttParser

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
      if (!this._address)
        this._address = `${socket.remoteAddress}:${socket.remotePort}`

      if (this.socket) {
        socket.setTimeout(10000)
        socket.on('close', () => this.broker?.closeClient(this))
        socket.on('timeout', () => this.broker?.closeClient(this))
        socket.on('error', () => this.broker?.closeClient(this))
        socket.on('data', (data: Buffer) => {
          this.parse(data)
          this.broker?.addStatistics('bytesReceived', data.length)
        })
      }
    }

    this.parser = new MqttParser()
    this.parser.on('packet', (msg: MqttMessage) => this.process(msg))
    this.parser.on('error', (err: Error) => {
      const dataError = err.message === 'Invalid input'
      if (!this.closing)
        this.disconnect(dataError ? MQTT_ERROR.MALFORMED_PACKET : MQTT_ERROR.UNSPECIFIED)
      if (!dataError)
        this.broker?.emit('error', err)
    })
  }

  clone (oldClient: BrokerClient): void {
    this.subscriptions = oldClient.subscriptions
    this.qosQueue = (oldClient as MqttClient).qosQueue
    this._queue = (oldClient as MqttClient)._queue
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

  write (msg: MqttMessage, data: Buffer, cb: (err?: Error) => void): void {
    if (this.socket)
      this.socket.write(data, cb)
    else
      cb(new Error('Invalid data'))
  }

  /** send packet to the client */
  send (msg: MqttMessage): void {
    if (this._queue.length >= this.maxQueueSize && msg.topic) {
      if (!this.cleanup(msg.topic))
        this.cleanup(0)
    }
    this._queue.push(msg)
    this._send()
  }

  private _send (): void {
    const msg = this._queue[this._queueIndex]
    if (!this._sending && msg) {
      this._sending = true

      const fixPayload: Buffer | string = 
        msg.payload === undefined ? '' :
        (msg.payload instanceof Buffer || typeof msg.payload === 'string') ? msg.payload :
        JSON.stringify(msg.payload)
      
      const data: Buffer = this.parser.generate(fixPayload === msg.payload ? msg : {...msg, payload: fixPayload})

      this.write(msg, data, err => {
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

    if (!this.socket)
      return false
    this.socket.end()
    this.socket = undefined
    return true
  }

  private _removeQosInbound (messageId: number) {
    // qosInbound = {messageId: {topic, payload, qos, clients:{clientId:messageId}}, ...}
    const msg = this.qosQueue?.inbound[messageId]
    if (msg) {
      for (const clientId in msg.clients) {
        const destMessageId = msg.clients[clientId]
        let destClient: BrokerClient | undefined = this.broker?.clients.get(clientId)
        if (destClient instanceof MqttClient) {
          const qosMsg = destClient.qosQueue.outbound[destMessageId]
          if (qosMsg?.client?.clientId === this.clientId)
            delete qosMsg.client
        }
      }
    }
  }

  private _removeQosOutbound (messageId: number, reasonCode?: number) {
    let msgOut = this.qosQueue?.outbound[messageId]
    if (msgOut && msgOut.client && this.qosQueue) {
      delete this.qosQueue.outbound[messageId]
      const origClient: MqttClient | undefined = this.broker?.clients.get(msgOut.client.clientId) as MqttClient
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
            this.send({ cmd: msgIn.qos === 2 ? 'pubcomp' : 'puback', messageId: origMessageId, reasonCode })
            delete origClient.qosQueue.inbound[origMessageId]
          }
        }
      }
    }
  }

  disconnect(reasonCode?: number): void {
    this.send({ cmd: 'disconnect', reasonCode })
    this.broker?.closeClient(this)
  }

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
          return this.disconnect(MQTT_ERROR.PROTOCOL)
        if (!msg.clientId || this.broker.clients.get(msg.clientId)?.intern)
          return this.disconnect(MQTT_ERROR.CLIENT_IDENTIFIER)

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
          this.send({ cmd: 'connack', reasonCode: MQTT_ERROR.BAD_USERNAME_PASSWORD })
          this.close()
          return
        }
        this.socket?.setTimeout((this.keepAlive || 30000) * 5 / 4)
        this.packetIndex = 0

        delete this._connecting
        this.send({ cmd: 'connack', sessionPresent: !newClient, reasonCode: 0 })
        break
      }
      case 'connack':
      case 'suback':
      case 'unsuback':
      case 'pingresp':
      case 'auth':
        this.disconnect(MQTT_ERROR.PROTOCOL)
        break
      case 'pingreq':
        this.send({ cmd: 'pingresp' })
        break
      case 'publish':
      case 'pubrel':
        if (msg.cmd === 'pubrel') {
          const messageId = msg.messageId || 0
          const msgIn = this.qosQueue?.inbound[messageId]
          // check for registered inbound message
          if (!msgIn) {
            this.send({ cmd: 'pubcomp', messageId, reasonCode: MQTT_ERROR.IDENTIFER_NOT_FOUND })
            return
          }
          // inbound message is allready released
          if (!msgIn.topic) {
            this.send({ cmd: 'pubcomp', messageId })
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
        this.send({ cmd: 'pubrel', messageId: msg.messageId })
        break
      case 'subscribe':
        granted = []
        msg.subscriptions?.forEach(sub => {
          granted.push(this.broker?.subscribe(sub, this) ?? MQTT_ERROR.UNSPECIFIED)
        })
        this.send({ cmd: 'suback', messageId: msg.messageId, granted: granted })
        break
      case 'unsubscribe':
        granted = []
        msg.unsubscriptions?.forEach(sub => {
          granted.push(this.broker?.unsubscribe(sub, this) ?? MQTT_ERROR.UNSPECIFIED)
        })
        this.send({ cmd: 'unsuback', messageId: msg.messageId, granted: granted })
        break
      case 'disconnect':
        this.broker?.removeClient(this)
        break
    }
  }

  parse(buffer: Buffer): void {
    this.parser.parse(buffer)
  }
}

export declare interface PolicyOptions {
  clientId?: string[]
  /** wildcard permissions for topics */
  permissions?: {[topic: string]: AclPermissions}
  /** $clientId|$username */
  prefix?: string
}

export declare interface UserInfo {
  policy: string
  group?: string
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
  handler?: Function
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

declare interface SubscribersItem {
  id: string
  qos: number
  nl?: boolean
  rap: boolean
  cb?: (messgae: MqttMessage) => boolean
}

/** MQTT-Broker server
 */
export class Broker extends EventEmitter {
  options: BrokerOptions
  startupTime: Date
  protocols: Function[]
  permissions: TopicCollection<TopicItem>
  subscribers: TopicCollection<SubscribersItem>
  data: TopicCollection<MqttMessage>
  modules: {[name: string]: any}
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

  protected _updateStatistics?: NodeJS.Timeout
  protected _expire: WeakMap<BrokerClient, NodeJS.Timeout> = new WeakMap()

  /** Start MQTT server
   */
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
      policy: {}, // {default: {prefix:'$username/$clientId', clientId:['prefix'], permissions:{'#':{read:true, publish:false, retain:false}}}
      permissions: {}, // {'topic': { retain:true }}
      users: {}, // {'user': {policy: 'default', password:'psw'}}
      log: 0,
      ...options
    }

    this.startupTime = new Date()
    this.protocols = [this.handler]
    this.permissions = new TopicCollection({})
    this.subscribers = new TopicCollection({ systemFilter: true })
    this.data = new TopicCollection({ systemFilter: true })
    this.modules = { broker: this }
    this.clients = new Map()

    this.clear()

    this.setOption('permissions', this.options.permissions)
    this.setOption('tls', this.options.tls)

    if (this.options.listen)
      this.listen()
  }

  /**
   * Add one time listener or call immediatelly for 'module:###', 'ready' 
   */
  once (name: string, cb: (...args: any[]) => void) {
    if (name.startsWith('module:') && this.modules[name.slice(7)])
      cb(this.modules[name.slice(7)])
    else if (name === 'ready' && this.servers.length)
      cb()
    else
      super.once(name, cb)
    return this
  }

  /**
   * Add listener and call immediatelly for 'module:###', 'ready' 
   * @param {string} name 
   * @param {function} cb 
   */
  on (name: string, cb: (...args: any[]) => void) {
    if (name.startsWith('module:') && this.modules[name.slice(7)])
      cb(this.modules[name.slice(7)])
    if (name === 'ready' && this.servers.length)
      cb()
    super.on(name, cb)
    return this
  }

  private _listenHandler (socket: net.Socket | tls.TLSSocket) {
    const destroy = () => socket.destroy()
    socket.setTimeout(10000, destroy)
    socket.on('error', destroy)
    socket.once('readable', () => {
      if (!('encrypted' in socket)) {
        const chunk = socket.read(1)

        socket.unshift(chunk)
        if (this.options.tls?.cert && chunk[0] === 22) {
          socket = new tls.TLSSocket(socket, {
            isServer: true,
            rejectUnauthorized: false,
            secureContext: tls.createSecureContext(this.options.tls)
          })
        } else {
          if (this.options.tls?.cert) {
            socket.destroy()
            return
          }
        }
      }
      socket.once('data', data => {
        socket.off('error', destroy)
        let i, n
        for (i = 0, n = this.protocols.length; i < n; i++) {
          if (this.protocols[i].call(this, socket, data))
            break
        }
        if (i === n)
          socket.end()
      })
    })
  }

  /**
   * Start TCP service
   */
  listen (options?: BrokerOptions) {
    let listen = options?.listen || ''
    // Close all listeners
    if (!options) {
      listen = this.options.listen || ''
      this.servers.splice(0).forEach(srv => srv.close())
    }
       
    // Start new listeners
    listen.toString().replace(/((\w+):\/\/)?(([^:,]*):)?([^:,]+)/, (_, __, proto, ___, host, port) => {
      const srv: net.Server = proto === 'tls' || options?.tls?.cert
        ? tls.createServer(options?.tls || this.options.tls || {}, (options?.handler || this._listenHandler).bind(this))
        : net.createServer((options?.handler || this._listenHandler).bind(this))
      const _onerr = (err: Error) => {
        this.emit('error', err, srv)
        srv.close()
      }
      srv.on('error', _onerr)
      srv.listen(port, host || '0.0.0.0', () => {
        this.servers.push(srv)
        srv.off('error', _onerr)
        const addr: net.AddressInfo = srv.address() as net.AddressInfo
        if (this.options.log)
          this.emit('log', `MQTT listen on ${addr.address}:${addr.port}`)
        this.emit('listen', addr)
        if (this.servers.length === 1)
          this.emit('ready')
      })
      return ''
    })
  }

  /**
   * Update option
   */
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
  close () {
    if (this.options.log)
      this.emit('log', 'Closing')
    clearTimeout(this._updateStatistics)
    this._updateStatistics = undefined
    this.servers.forEach(srv => srv.close())
    this.clear()
    Object.entries(this.modules).forEach(([name, mod]) => {
      if (mod && mod !== this && mod.close)
        mod.close()
    })
    this.emit('close')
  }

  module (name: string, obj: Function |
    {plugin: (broker: Broker, config: any) => any} |
    {setBroker: (broker: Broker, config: any) => void}, config: any) {
    if (this.options.log)
      this.emit('log', `MODULE ${name}`)
    if (typeof obj === 'function')
      obj = obj(this, config) || {}
    else if ('plugin' in obj)
      obj = obj.plugin(this, config) || obj
    else if ('setBroker' in obj)
      obj.setBroker(this, config)
    if (obj) {
      this.modules[name] = obj
      this.emit('module', name, obj)
      this.emit('module:' + name, obj)
    }
    return obj
  }

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

  /** Add client to the broker
   * @param {BrokerClient} client - client object
   * @returns {boolean} `true` if client was created
   */
  addClient (client: BrokerClient): boolean {
    if (client instanceof MqttClient) {
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
      if (client instanceof MqttClient)
        return false
    }

    if (!client.group && client.policy?.group)
      client.group = client.policy.group
    if (!client.prefix && client.policy?.prefix)
      client.prefix = (client.policy.prefix.replace(/\$(\S+)/g, (name: string) => this._subOption(client, name) || '') + '/').replace(/^\/+/, '')
    if (client.prefix && !client.prefix.endsWith('/'))
      client.prefix += '/'

    if (client.policy?.globalPermissions)
      Object.entries(client.policy.globalPermissions).forEach(([n, v]) => {
        if (n.startsWith('/'))
          n.substring(1)
        else
          n = client.prefix + n
        this.permissions.add(n, { ...v, id: client.id })
      })

    if (client.policy?.permissions && !(client.policy.permissions instanceof TopicCollection)) {
      const col = new TopicCollection<AclPermissions>({ systemFilter: true })
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
      client.policy.publications.forEach((pub: MqttMessage) => this.publish(pub, client))

    //client.keepAlive = this.connectionTimeout / 2
    this.emit('clients/connect', { clientId: client.clientId, username: client.username, address: client.address })
    return true
  }

  /**
   * Remove client from the broker
   */
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

  /**
   * If client sends disconnect message
   */
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

  /** @description Publish topic=payload */
  set (topic: string, payload: string | Buffer, retain?: boolean, qos?: number) {
    this.publish({
      topic: topic,
      payload: payload,
      retain: retain === undefined ? false : retain,
      qos: qos || 0
    })
  }

  /** @description Get retain publication
   * @param {string} topic
   * @return {Object} publication object: {topic, payload, retain}
   */
  get (topic: string): MqttMessage | undefined {
    return this.data.get(topic)
  }

  /** Update policy */
  setPolicy (policy: { [id: string]: PolicyOptions }) {
    this.options.policy = { ...policy }
    if (!this.options.policy.default)
      this.options.policy.default = { prefix: '$username/$clientId' }
    if (!this.options.policy.admin)
      this.options.policy.admin = { prefix: '', permissions: { '#': { read: true, publish: true } } }
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
          ? (policy.prefix.replace(/\$(\S+)/g, (name: string) => this._subOption(user, name) || this._subOption(client, name) || '') + '/').replace(/^\/+/, '') : ''
      }
    }
    if (!client.auth)
      throw new Error('Access denied')
    return client
  }

  addStatistics (id: string, value: number) {
    if (id in this.statistics)
      (this.statistics as any)[id] += value
  }

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
  private _fixTopic (client: BrokerClient, topic: string, permissionId?: string, defaultPermission?: boolean): { topic: string, reasonCode: number } {
    let reasonCode = client ? MQTT_ERROR.NOT_AUTHORIZED : 0
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
          topic.indexOf('//') < 0) ? 0 : MQTT_ERROR.NOT_AUTHORIZED
      if (!reasonCode && permissionId && !hasPrefix)
        reasonCode = this._permission(client, topic, permissionId, defaultPermission) ? 0 : MQTT_ERROR.NOT_AUTHORIZED
    }
    return {
      topic,
      reasonCode
    }
  }

  /** Process incomming messages
   * @param {Object} message
   * @param {boolean} intern - used for recursive processing
  */
  process (message: MqttMessage, intern?: boolean): boolean | undefined {
    if (intern)
      return true
    if (message.retain) {
      const topic = message.topic || ''
      let data = this.data.get(topic)
      if (!data)
        data = this.data.set(topic, { topic, retain: true, qos: 0 })
      if (data.payload === message.payload)
        return false
      data.payload = message.payload
    }
  }

  /** Publish packet
   * @param {Object} msg
   * @param {string} msg.topic - message topic
   * @param {string|Buffer} msg.payload - message payload
   * @param {boolean} [msg.retain=false] - message qos
   * @param {number} [msg.qos=0] - message qos
   * @param {BrokerClient?} client - client or undefined if system message
   */
  publish (msg: MqttMessage, client?: BrokerClient) {
    let topic: string = msg.topic as string, reasonCode = 0, retain = msg.retain

    // if client is intern module, can publish qos>0, receive only qos=0
    msg.qos = msg.qos || 0

    if (client && !msg.clients) {
      this.statistics.publishReceived++

      ({ topic, reasonCode } = this._fixTopic(client, topic, 'publish'))
      if (!reasonCode)
        retain = this._permission(client, topic, 'retain', !!retain)
      if (msg.qos === 2 && client.maximumQos === 2) {
        if (client instanceof MqttClient) {
          if (!reasonCode && client.qosQueue.inbound[msg.messageId || 0])
            reasonCode = MQTT_ERROR.MESSAGEID_INUSE
          if (!reasonCode)
            client.qosQueue.inbound[msg.messageId || 0] = { topic: topic, payload: msg.payload, qos: msg.qos, retain, messageId: msg.messageId || 0, clients: {} }
        }
        if (reasonCode)
          this.statistics.publishDropped++
        client.send({ cmd: 'pubrec', messageId: msg.messageId, reasonCode })
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
          if (hasPrefix || this._permission(subClient, topic, 'read')) { // get all prefixed messages by policy, check acl read permission
            let subtopic = processMessage.topic
            if (hasPrefix)
              subtopic = subtopic.substring(subClient.prefix.length)
            const message = { cmd: 'publish', qos, reference: processMessage.topic, topic: subtopic, payload: processMessage.payload, retain: sub.rap ? msg.retain : false }
            if (sub.cb && sub.cb(message) === false) {
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
        if (!(subClient instanceof MqttClient))
          return
        subClient.send(message)
        if (message.messageId && client?.maximumQos) {
          subClient.qosQueue.outbound[message.messageId || 0] = { qos: message.qos, client: client && { clientId: client.clientId, messageId: msg.messageId } }
          if (!qosClients)
            qosClients = {}
          qosClients[subClient.clientId] = message.messageId
        }
      })
    }

    if (client instanceof MqttClient && client.maximumQos && msg.qos) {
      if (qosClients) {
        client.qosQueue.inbound[msg.messageId || 0] = { qos: msg.qos, clients: qosClients }
      } else {
        delete client.qosQueue.inbound[msg.messageId || 0]
        client.send({ cmd: msg.qos === 2 ? 'pubcomp' : 'puback', messageId: msg.messageId, reasonCode })
      }
    }
  }

  /** subscribe single client topic
   * @param {Object|string} sub - subscription
   * @param {string} sub.topic - subscription topic (with wildcards: #,+)
   * @param {number} [sub.qos=0] - subscription qos
   * @param {BrokerClient} client - client
   * @param {function} [cb] - optional callback: cb(message) used for modules, if returns false, breaks processing
   */
  subscribe (sub: { topic: string, qos?: number, rh?: number, nl?: boolean, rap?: boolean } | string, client: BrokerClient, cb?: (message: MqttMessage) => boolean): number {
    if (typeof sub === 'string')
      sub = { topic: sub }
    if (client.subscriptions.includes((sub as any).topic))
      return sub.qos || 0
    const { topic, reasonCode } = this._fixTopic(client, sub.topic, 'read')
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
            if (hasPrefix || this._permission(client, topic, 'read')) { // get all prefixed messages by policy, check acl read permission
              let subtopic: string = pub.topic as string
              if (hasPrefix)
                subtopic = subtopic.substring(client.prefix.length)

              const message = { cmd: 'publish', qos: Math.min(pub.qos || 0, sub.qos || 0), reference: pub.topic, topic: subtopic, payload: pub.payload, retain: sub.rap || false }
              if (!cb || cb(message) !== false) {
                if ((this.options.log || 0) > 2)
                  this.emit('log', '> ' + client.id + ' topic:' + message.topic + ' payload:' + message.payload)
                client.send(message)
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
  private _permission (client: BrokerClient, topic: string, permissionId: string, defaultPermission: boolean = true) {
    //defaultPermission = typeof defaultPermission === 'undefined' ? true : defaultPermission
    const acl = this.permissions.getOption(topic, permissionId, defaultPermission)
    return client?.policy?.permissions instanceof TopicCollection ? client.policy.permissions.getOption(topic, permissionId, acl) : acl
  }

  /** mqtt protocol handler */
  handler (socket: net.Socket, data: Buffer) {
    if (data && data[0] !== 0x10) // first packet is allways connect
      return
    new MqttClient({broker: this, socket})
    return true
  }
}
