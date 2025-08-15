import assert from 'assert'

const test: {
  (name: string, fn?: Function): void
  skip: (name: string, ...args: any) => void
  mode: (mode: 'stop' | 'continue') => void
  after: (fn: Function) => void
  run: () => Promise<void>
} = global.test = (() => {
  let tests: {name: string, fn?: Function}[] = [], _mode: 'stop' | 'skip' | 'continue' = 'continue', _skip: boolean, _run: boolean, _after: Function | undefined
  const test = (name: string, fn?: Function) => {
    tests.push({ name, fn })
    !_run && tests.length === 1 && process.nextTick(test.run)
  }
  test.skip = (name: string, ...args: any) => name ? test(name) : _skip = true
  test.mode = (mode: 'stop' | 'skip' | 'continue') => _mode = mode
  test.after = (fn: Function) => _after = fn
  test.run = async () => {
    if (_run) return
    _run = true
    const {log, error, warn} = console
    let count = 0, fail = 0, lastError: Error | undefined 
    const after = async () => { try { await _after?.() } catch (e) { error(e.stack) } _after = undefined }
    const run = async (prefix: string) => {
      for (const {name, fn} of tests) {
        const stime = performance.now(), out = (pre: string, msg?: string, post?: string) => log(`${prefix}\x1b[${pre} ${name}${msg?': '+msg:''}\x1b[90m (${post||(performance.now() - stime).toFixed(1)+'ms'})\x1b[0m`)
        try {
          count++
          tests = []
          if (!(_skip = !fn)) {
            await fn()
            await after()
          }
          if (_skip) {
            out('90m—', '', 'skipped')
            count--
          } else if (tests.length) {
            out('32m—')
            await run(prefix + '  ')
            if (_mode === 'stop' && lastError)
              return
            lastError = undefined
          } else
            out('32m✔')
        } catch (e) {
          fail++
          out('31m✘', e.message)
          e.name !== 'AssertionError' && error(e.stack);
          await after()
          lastError = e
          if (_mode !== 'continue')
            return
        }
      }
    }
    tests.length && await run('');
    _run = false
    if (count) {
      const l = '—'.repeat(16)+'\n'
      log(`\x1b[${fail?'33m'+l+'✘':'32m'+l+'✔'} ${count-fail}/${count} ${fail?'FAILED':'SUCCESS'}\x1b[0m`)
      setTimeout(() => {
        warn('Active resources:', ...process.getActiveResourcesInfo().filter(n => n !== 'CloseReq' && !n.endsWith('Wrap')))
        process.exit(1)
      }, 1000).unref()
    }
  }
  return test
})()

import { Broker, BrokerClient, MqttParser, MqttGenerator } from './mqtt.ts'
import type { MqttMessage, BrokerOptions } from './mqtt.ts'

test('mqtt', () => {
  
const mqttMessages : [string, string, string, MqttMessage][] = [
  ['connect', 'EDMABE1RVFQFwgA8GBEAAAEsJgAIc2VydmVySWQABnNlcnZlcgAEdGVzdAADdXNyAANwc3c=', 'EBoABE1RVFQEwgA8AAR0ZXN0AAN1c3IAA3Bzdw==', {cmd: 'connect', protocol: 'MQTT', protocolVersion: 5, clean: true, keepalive: 60, username: 'usr', password: 'psw', clientId: 'test', properties: {sessionExpiryInterval: 300, userProperties: {serverId: 'server'}}}],
  ['connack', 'IBkBgBYhQAAmAAhzZXJ2ZXJJZAAGc2VydmVy', 'IAIBBQ==', {cmd: 'connack', sessionPresent: true, reasonCode: 128, properties: {receiveMaximum: 16384, userProperties: {serverId: 'server'}}}],
  ['subscribe', 'ghoAKhALkQEmAAR0ZXN0AAR0ZXN0AAR0ZXN0AA==', 'ggkAKgAEdGVzdAA=', {cmd: 'subscribe', messageId: 42, properties: {subscriptionIdentifier: 145, userProperties: {test: 'test'}}, subscriptions: [{topic: 'test', qos: 0, nl: false, rap: false, rh: 0}]}],
  ['suback', 'kBsAKhQfAAR0ZXN0JgAEdGVzdAAEdGVzdAABAoA=', 'kAYAKgABAoA=', {cmd: 'suback', messageId: 42, properties: {reasonString: 'test', userProperties: {test: 'test'}}, granted: [0, 1, 2, 128]}],
  ['unsubscribe', 'oh8AKg0mAAR0ZXN0AAR0ZXN0AAR0ZXN0AAdhL3RvcGlj', 'ohEAKgAEdGVzdAAHYS90b3BpYw==', {cmd: 'unsubscribe', messageId: 42, properties: {userProperties: {test: 'test'}}, unsubscriptions: ['test', 'a/topic']}],
  ['unsuback', 'sBoAKhQfAAR0ZXN0JgAEdGVzdAAEdGVzdAECAw==', 'sAIAKg==', {cmd: 'unsuback', messageId: 42, properties: {reasonString: 'test', userProperties: {test: 'test'}}, granted: [1, 2, 3]}],
  ['publish', 'NCEABHRlc3QAKhQmAAR0ZXN0AAR0ZXN0AwAEdGVzdHRlc3Q=', 'NAwABHRlc3QAKnRlc3Q=',   {cmd: 'publish', dup: false, qos: 2, retain: false, topic: 'test', messageId: 42, payload: Buffer.from('test'), properties: {contentType: 'test', userProperties: {test: 'test'}}}],
  ['puback', 'QBgAKhAUHwAEdGVzdCYABHRlc3QABHRlc3Q=', 'QAIAKg==', {cmd: 'puback', messageId: 42, reasonCode: 16, properties: {reasonString: 'test', userProperties: {test: 'test'}}}],
  ['pubrec', 'UBgAKhAUHwAEdGVzdCYABHRlc3QABHRlc3Q=', 'UAIAKg==', {cmd: 'pubrec', messageId: 42, reasonCode: 16, properties: {reasonString: 'test', userProperties: {test: 'test'}}}],
  ['pubrel', 'YhgAKhAUHwAEdGVzdCYABHRlc3QABHRlc3Q=', 'YgIAKg==', {cmd: 'pubrel', messageId: 42, reasonCode: 16, properties: {reasonString: 'test', userProperties: {test: 'test'}}}],
  ['pubcomp', 'cBgAKhAUHwAEdGVzdCYABHRlc3QABHRlc3Q=', 'cAIAKg==', {cmd: 'pubcomp', messageId: 42, reasonCode: 16, properties: {reasonString: 'test', userProperties: {test: 'test'}}}],
  ['pingreq', 'wAA=', 'wAA=', {cmd: 'pingreq'}],
  ['pingresp', '0AA=', '0AA=', {cmd: 'pingresp'}],
  ['disconnect', '4CIAIBEAAACRHwAEdGVzdCYABHRlc3QABHRlc3QcAAR0ZXN0', '4AA=', {cmd: 'disconnect', reasonCode: 0, properties: {sessionExpiryInterval: 145, reasonString: 'test', serverReference: 'test', userProperties: {test: 'test'}}}],
  ['auth', '8CQAIhUABHRlc3QWAAQAAQIDHwAEdGVzdCYABHRlc3QABHRlc3Q=', '', {cmd: 'auth', reasonCode: 0, properties: {authenticationMethod: 'test', authenticationData: Buffer.from([0, 1, 2, 3]), reasonString: 'test', userProperties: {test: 'test'}}}]
]

test('parser-buffer', () => {
  mqttMessages.forEach(m => {
    const buf: Buffer = Buffer.from(m[1], 'base64')
    const parser: MqttParser = new MqttParser(5)
    let msg: MqttMessage | Error | undefined
    parser.on('error', e => msg = e)
    parser.on('packet', m => msg = m)
    for (let i = 1; i <= 5; i++) {
      parser.clear()
      for (let j = 0; j < buf.length; j += i) {
        parser.parse(buf.subarray(j, j + i))
        assert(!(msg instanceof Error), (msg as Error)?.message)
      }
      assert(msg, 'Parser failed on i:' + i)
      assert.deepEqual(msg, m[3], 'Failed parse on i:' + i)
    }
  })
})

test('parse/generate', () => {
  function mqttParse(ver: number, data: string, name: string) {
    const parser = new MqttParser(ver)
    let msg: MqttMessage | undefined
    parser.on('error', e => assert(false, 'Error parsing ' + name + ': ' + e.message))
    parser.on('packet', m => msg = m)
    parser.parse(Buffer.from(data, 'base64'))
    assert(msg !== undefined, 'Parser failed on ' + name)
    return msg
  }
  function mqttGenerate(ver: number, data: MqttMessage, name: string) {
    const generator = new MqttGenerator(ver)
    return generator.generate(data).toString('base64')
  }
  mqttMessages.forEach(([cmd, msg5, msg4, obj]) => {
    let msg = mqttParse(5, msg5, cmd)
    assert(msg.cmd === cmd, 'Failed parse v5 ' + cmd)
    assert.deepEqual(msg, obj, 'Failed parse v5 ' + cmd)
    assert.deepEqual(mqttParse(5, mqttGenerate(5, msg, cmd), cmd), obj, 'Failed generate v5 ' + cmd)
    if (msg4) {
      msg = mqttParse(4, msg4, cmd)
      assert(msg4 === mqttGenerate(4, msg, cmd), 'Failed parse v4 ' + cmd)
      msg = mqttParse(5, msg5, cmd)
      if (msg.cmd === 'connect')
        msg.protocolVersion = 4
      assert(msg4 === mqttGenerate(4, msg, cmd), 'Failed generate v4 ' + cmd)
    }
  })
})

})


import net from 'net'
import tls from 'tls'

test('mqtt-broker', async () => {


const port = 18883
const timeout = 5000
const config: BrokerOptions = {
  listen: 'tcp://127.0.0.1:'+port+',tls://127.0.0.1:'+(port+1),
  tls: {
    cert: `-----BEGIN CERTIFICATE-----
MIIBMjCB5aADAgECAhQgMIQegaO3kPcf/FzAZ4Pz9i2c1zAFBgMrZXAwDzENMAsG
A1UEAwwEbXF0dDAeFw0yNTA4MTQwMTUwMDdaFw00NTA4MDkwMTUwMDdaMA8xDTAL
BgNVBAMMBG1xdHQwKjAFBgMrZXADIQD+sKX38scriXOHhq6Xzkuh3QJ4ZIRygrI3
scsR+lcF9KNTMFEwHQYDVR0OBBYEFDIuWHScUvKSJlD9kItHhfCWepopMB8GA1Ud
IwQYMBaAFDIuWHScUvKSJlD9kItHhfCWepopMA8GA1UdEwEB/wQFMAMBAf8wBQYD
K2VwA0EAbVOtGpYSZAVuSTraxljR9hSLJUDs2hkUOJ20FelOOdjVFhbZn4MoTo/V
htQm7dqZcsbFhuYNWIcaGxGZ1KkbAQ==
-----END CERTIFICATE-----`,
    key: '-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIM6/lDgXiGttGSLnmK3nwkzfe6LGGBlNpkbKYv6GX58O\n-----END PRIVATE KEY-----'  
  },
  policy: {
    usr: {
      prefix: 'devices/$clientId',
      permissions: {
        'test': { publish: false, subscribe: false }
      }
    },
    default: {
    }
  }, // [{prefix:'$user/$clientId', user: ['prefix'], clientId:['prefix'], permissions:{'topic':{subscribe:true, publish:false}}]
  users: {
    usr: { policy: 'usr', password: 'psw' },
    usr1: { policy: 'usr', password: 'psw1' }
  },
  log: 2
}
const broker = new Broker(config)

class MqttConn {
  socket: net.Socket | tls.TLSSocket | undefined
  result: any[]
  parser: MqttParser
  secure: boolean = false

  async connect (object?: any) {
    this.socket?.destroy()
    this.socket = undefined
    this.result = []

    this.parser = new MqttParser()
    this.parser.on('packet', packet => this.result.push(packet))
    this.parser.on('error', err => this.result.push({ cmd: 'error', error: err }))

    const res = await new Promise((resolve, reject) => {
      const cb = (err?: Error) => {
        if (err) reject(err)
        else resolve(true)        
      }
      let socket
      if (this.secure)
        this.socket = socket = tls.connect({
          port: port + 1,
          host: '127.0.0.1',
          rejectUnauthorized: false,
          timeout,
        }, cb)
      else
        this.socket = socket = net.connect({ port, host: '127.0.0.1', timeout }, cb)
      this.socket.setTimeout(timeout)
      socket.on('timeout', () => {
        if (this.socket === socket) {
          this.parser.emit('packet', { cmd: 'timeout' })
          this.socket?.destroy()
        }
      })
      socket.on('close', () => {
        if (this.socket === socket)
          this.parser.emit('packet', { cmd: 'close' })
      })
      socket.on('data', (data: Buffer) => {
        if (this.socket === socket)
          this.parser.parse(data)
      })
    })
    if (!object)
      return res
    return this.send(object)
  }
  close () {
    this.socket?.destroy()
  }
  recv (cmd: string): Promise<MqttMessage> {
    return new Promise((resolve, reject) => {
      for (let i = 0; i < this.result.length; i++) {
        let packet = this.result[i]
        if (packet.cmd === cmd) {
          this.result.splice(0, i)
          return resolve(packet)
        }
        if (packet.cmd === 'close')
          return reject(new Error('closed'))
      }
      let timer: NodeJS.Timeout | undefined, func: (packet: MqttMessage) => void, cleanup = () => {
        clearTimeout(timer)
        this.parser.off('packet', func)
      }
      func = packet => {
        this.result.splice(0, this.result.length)
        if (packet.cmd === cmd) {
          cleanup()
          return resolve(packet)
        }
        if (packet.cmd === 'close') {
          cleanup()
          return reject(new Error('closed'))
        }
      }
      timer = setTimeout(() => {
        cleanup()
        reject(new Error('timeout'))
      }, 2000)
      this.parser.on('packet', func)
    })
  }
  send (object) {
    return new Promise((resolve, reject) => {
      if (!this.socket)
        reject(new Error('Not connected'))
      else {
        const data = Array.isArray(object)
          ? Buffer.concat(object.map(p => this.parser.generate(p)))
          : this.parser.generate(object)
        this.socket.write(data, (err) => {
          if (err) reject(err)
          else resolve(true)
        })
      }
    })
  }  
}
const mqtt = new MqttConn()

/// //////////////////////////////////////////////////////////////////////////
// MQTT
/// //////////////////////////////////////////////////////////////////////////
test.mode('stop')
test('mqtt-broker', async () => {
  test('auth', async () => {
    const client = new BrokerClient({username: 'usr', id: 'test'})
    const err = await broker.auth(client, 'psw').then(() => {}).catch(e => e.message)
    assert(!err, err)
  })
  test('Connect', () => {
    test('Valid', async () => {
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr', password: 'psw' })
      let connack = await mqtt.recv('connack')
      assert.equal(connack.returnCode || connack.reasonCode, 0)
      assert(broker.clients.get('test'), 'Client was not added to clients')
      broker.clear()
    })
    test('Invalid', async () => {
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr2', password: 'psw2' })
      let connack = await mqtt.recv('connack')
      assert.equal(connack.reasonCode, 0x86, 'returnCode should be 4 (Bad username/password)') // MQTT v3.1.1 code 4
      assert.equal(broker.clients.get('test'), undefined, 'Client should not be added to clients')
    })
    test('TLS', async () => {
      mqtt.secure = true
      test.after(() => mqtt.secure = false)
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr', password: 'psw' })
      let connack = await mqtt.recv('connack')
      assert.equal(connack.reasonCode, 0, 'returnCode should be 0')
      assert(broker.clients.get('test'), 'Client was not added to clients')
      broker.clear()
    })
  })

  test('Publish', () => {
    test('QOS=0', async () => {
      let pub: any, check = (msg: MqttMessage) => { pub = msg.payload }
      broker.on('publish', check)
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr', password: 'psw' })
      await mqtt.recv('connack')
      await mqtt.send({ cmd: 'publish', topic: 'data/test', payload: 'data' })
      await mqtt.send({ cmd: 'disconnect' })
      await mqtt.recv('close')
      broker.off('publish', check)
      assert(pub, 'Publish was not received')
      assert.equal(pub, 'data', 'Publish - QOS=0: payload should be \'data\'')
      assert.equal(broker.clients.get('test'), undefined, 'Client was not removed from clients')
      broker.clear()
    })
    test('QOS=0,Retain', async () => {
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr', password: 'psw' })
      await mqtt.recv('connack')
      await mqtt.send([
        { cmd: 'publish', topic: 'data/test', payload: 'data', retain: true },
        { cmd: 'disconnect' }
      ])
      await mqtt.recv('close')
      const retained = broker.get('devices/test/data/test')
      assert(retained, 'message should exist')
      assert.equal(retained.payload, 'data', 'payload should be \'data\'')
      assert.equal(retained.retain, true, 'retain flag should be true')
      broker.clear()
    })
    test('QOS=1', async () => {
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr', password: 'psw' })
      await mqtt.recv('connack')
      await mqtt.send({ cmd: 'publish', topic: 'data/test', payload: 'data', retain: true, qos: 1, messageId: 1 })
      let puback = await mqtt.recv('puback')
      assert.equal(puback.messageId, 1, 'Publish - QOS=1: messageId should be 1')
      const qos1Message = broker.get('devices/test/data/test')
      assert(qos1Message, 'message should exist')
      assert.equal(qos1Message.payload, 'data', 'payload should be \'data\'')
      assert.equal(qos1Message.retain, true, 'retain flag should be true')
      broker.clear()
    })
    test('Prefix', async () => {
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr1', password: 'psw1' })
      await mqtt.recv('connack')
      await mqtt.send([
        { cmd: 'publish', topic: 'test2', payload: 'data', retain: true },
        { cmd: 'disconnect' }
      ])
      await mqtt.recv('close')
      assert(broker.get('devices/test/test2'))
      assert.equal(broker.get('devices/test/test2')?.topic, 'devices/test/test2', 'topic should be \'devices/test/test2\'')
      assert.equal(broker.get('devices/test/test2')?.payload, 'data', 'payload should be \'data\'')
      assert.equal(broker.get('devices/test/test2')?.retain, true, 'retain flag should be true')
      broker.clear()
    })
    test('ACL', async () => {
      await mqtt.connect({ cmd: 'connect', clientId: 'test', username: 'usr', password: 'psw' })
      await mqtt.recv('connack')
      await mqtt.send({ cmd: 'publish', topic: 'test', payload: 'data', qos: 1, retain: true, messageId: 1 })
      let puback = await mqtt.recv('puback')
      assert.equal(puback.messageId, 1, 'messageId should be 1')
      assert.equal(broker.get('devices/test/test'), undefined, 'devices/test/test should not exist')
      broker.clear()
    })
  })

  test('close', async () => {
    await broker.close()
    mqtt.close()
    assert.equal(broker.clients.size, 0, 'All clients should be closed')
    assert.equal(mqtt.socket?.destroyed, true, 'MQTT socket should be destroyed')
  })
})

})
