const S3og = require('../lib/s3og')
// opts = {cleanup, onStop, onDead}
S3og.exitHandler()
const Nats = require('nats')
const fs = require('fs')

async function pingpong (nats) {
  const pingpongService = new S3og(
    'pingpong', {
      defaultRequestTimeout: 1000
    }
  )
  const TEST_GROUP = 'test.group'
  const controllers = S3og.readControllersFromDir({
    path: './',
    mask: /test.pingpong.*/,
    group: TEST_GROUP
  })
  controllers.forEach(c => pingpongService.use(c))
  // pingpongService.on('TASK', task => console.log(task))
  // pingpongService.on('TASK_END', taskResult => console.log(taskResult))
  pingpongService.on('STOP', reason => console.log(`${pingpongService.name} stop:`, reason))
  console.log('start', new Date())
  const proxy = pingpongService.go(nats)
  console.log(pingpongService.controllers)
  console.log(await proxy.ask('test.pingpong.ping', { message: 'ping' }))
}

async function sink (nats) {
  const sinkService = new S3og('sink')
  // all controllers runned in same process. just for a demonstration
  for (let i in [1, 2, 3, 4, 5]) {
    console.log('producer', i)
    sinkService.use({
      subject: 'test.sink.producer',
      handler: require('./test.sink.producer')
    })
  }
  // sinkService.on('TASK', task => console.log(task))
  // sinkService.on('TASK_END', taskResult => console.log(taskResult))
  sinkService.on('STOP', reason => console.log(`${sinkService.name} stop:`, reason))
  console.log('start', new Date())
  const proxy = sinkService.go(nats)
  console.log(sinkService.controllers)
  console.log('must be only 3 random values:', await proxy.sink('test.sink.producer', null, 1000, 3))
  console.log('must be 5 random values:', await proxy.sink('test.sink.producer', null, 1000, 99))
  console.log('must be 0 random values:', await proxy.sink('test.sink.producer', null, 1, 99))
  sinkService.stop()
}

const nats = Nats.connect({
  url: 'tls://gnatsd.local:4222',
  rejectUnauthorized: true,
  maxReconnectAttempts: 20,
  reconnectTimeWait: 500,
  json: true,
  tls: {
    key: fs.readFileSync('/home/gb/tools/EasyRSA-3.0.4/pki/private/wallets-test.key'),
    cert: fs.readFileSync('/home/gb/tools/EasyRSA-3.0.4/pki/issued/wallets-test.crt'),
    ca: [fs.readFileSync('/home/gb/tools/EasyRSA-3.0.4/pki/ca.crt')]
  }
})
nats.on('connect', () => {
  S3og.exitHandler({
    cleanup: () => nats.close(),
    onStop: event => console.log('app stop:', event),
    onDead: error => console.log('app dead:', error)
  })
  pingpong(nats)
  .then(() => sink(nats))
  .then(() => process.exit())
  .catch(e => process.exit(e))
})
