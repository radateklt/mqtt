## MQTT Broker / Client

Lightweight MQTT-Broker and MQTT-Client

supported MQTT features:
  - MQTT 3.1.1, 5.0
  - connect: username, password, will, clean, keepalive, sessionExpiryInterval
  - publish: qos=0-2, retain
  - subscribe: qos=0-2, rh (Retain Handling), nl (No Local), rap (Retain as Published)
  - policies: permissions, topic prefix, init subscriptions, init publications

### Usage example for MQTT broker:

```ts
const broker = new Broker({
  listen: 1883,
  policy: {
    usr: {
      prefix: 'devices/$clientId',
      permissions: {
        'test': { publish: false, subscribe: false }
      }
    }
  },
  users: {
    usr: { policy: 'usr', password: 'psw' }
  }
})
broker.on('publish', msg => console.log(`${msg.topic}: ${msg.payload}`))

// Custom broker client
class ClockProvider extends BrokerClient {
  id: 'clock-provider'
  intern: true
  constructor(broker: Broker) {
    super()
    broker.addClient(this)
    broker.subscribe('clock/change', this, (msg) => {
      console.log('requested clock change')
      return false // drop message
    })
    setInterval(() => broker.publish({
      topic: 'clock/time',
      payload: new Date().toJSON().slice(0,19).replace('T',' ')
    }, this), 1000)
  }
}
new ClockProvider(broker)
```

### Usage example for MQTT connection client:

```ts
const conn = new MqttConnection({
  url: 'mqtts://example.com:8883'
  clientId: 'mqtt-client',
  username: 'mqtt-user',
  password: 'secret',
  protocolVersion: 5
})
conn.subscribe('devices/#', 0,
  (msg) => console.log(`${msg.topic}: ${msg.payload}`))
conn.subscribe('messages/#')
conn.on('connect', () => console.log('connected'))
conn.on('publish', (msg) => {})
conn.on('topic:messages/error',
  (msg) => console.error(msg.payload.toString()))
conn.on('disconnect',
  (reason) => console.log(`disconnected: ${reason}`))
conn.on('close', () => console.log('closed'))
conn.connect()
...
conn.publish('message/log', 'Processing...')
```