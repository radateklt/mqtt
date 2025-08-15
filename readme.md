## HTTP MicroServer

Lightweight MQTT-Broker

supported MQTT features:
  - MQTT 3.1.1, 5.0
  - connect: username, password, will, clean, keepalive, sessionExpiryInterval
  - publish: qos=0-2, retain
  - puback:
  - subscribe: qos=0-2, rh (Retain Handling), nl (No Local), rap (Retain as Published)
  - unsubscribe:
  - policies: permissions, topic prefix, init subscriptions, init publications

### Usage examples:

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

```