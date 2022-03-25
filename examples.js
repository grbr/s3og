const S3og = require('./lib/s3og')
// opts = {cleanup, onStop, onDead}
S3og.exitHandler()
const Nats = require('nats')

async function pingpong (nats) {
  hr()
  const pingpongService = new S3og(
    'pingpong', {
      defaultRequestTimeout: 1000
    }
  )
  const TEST_GROUP = 'test.group'
  const controllers = S3og.readControllersFromDir({
    path: './examples',
    mask: /test.pingpong.*/,
    group: TEST_GROUP
  })
  controllers.forEach(c => pingpongService.use(c))
  // pingpongService.on('TASK', task => console.log(task))
  // pingpongService.on('TASK_END', taskResult => console.log(taskResult))
  pingpongService.on('STOP', reason => console.log(`${pingpongService.name} stop:`, reason))
  console.log('start', new Date())
  const ether = pingpongService.go(nats)
  console.log(pingpongService.controllers)
  console.log(await ether.ask('test.pingpong.ping', { message: 'ping' }))
}

async function sink (nats) {
  hr()
  const sinkService = new S3og('sink')
  // all controllers runned in same process. just for a demonstration
  for (const i in [1, 2]) {
    console.log('producer', i)
    sinkService.use({
      subject: 'test.sink.producer',
      handler: require('./examples/test.sink.producer')
    })
  }
  // sinkService.on('TASK', task => console.log(task))
  // sinkService.on('TASK_END', taskResult => console.log(taskResult))
  sinkService.on('STOP', reason => console.log(`${sinkService.name} stop:`, reason))
  console.log('start', new Date())
  const ether = sinkService.go(nats)
  console.log(sinkService.controllers)
  console.log('must be only 1 random values:', await ether.sink('test.sink.producer', null, 1000, 1))
  console.log('must be 2 random values:', await ether.sink('test.sink.producer', null, 1000))
  console.log('must be 0 random values:', await ether.sink('test.sink.producer', null, 1))
  console.log('must be 2 errors:', await ether.sink('test.sink.producer', { beBad: true }, 1000, 1))
  sinkService.stop('sink task end')
}

async function badSubject (nats) {
  hr()
  const badSubject = new S3og('bad-subject')
  badSubject.on('STOP', reason => console.log(`${badSubject.name} stop:`, reason))
  const ether = badSubject.go(nats)
  try {
    console.log('error must have property subject="unexisting.subject"')
    await ether.ask('unexisting.subject', {}, {}, 1)
  } catch (err) {
    console.log(err)
  }
  badSubject.stop('badSubject test end')
}

async function checkLogger (nats) {
  hr()
  const checkLoggerService = new S3og('checkLoggerService')
  const logger = require('pino')()
  checkLoggerService.attachLogger(logger)
  checkLoggerService.use({
    subject: 'some.task',
    handler: async (ether, request, subject) => {
      return 'task result'
    }
  })
  console.log('must be 3 log entries:')
  const ether = checkLoggerService.go(nats)
  await ether.ask('some.task')
  await ether.sink('instance', null, 100, 1)
  checkLoggerService.stop('checkLogger test end')
}

async function checkInstance (nats) {
  hr()
  const checkInstanceService = new S3og('checkInstanceService')
  console.log('must be instance response:')
  const ether = checkInstanceService.go(nats)
  console.log(await ether.sink('instance', null, 100, 2))
  checkInstanceService.stop('checkInstance test end')
}

const nats = Nats.connect({
  url: 'nats://localhost:4222',
  maxReconnectAttempts: 20,
  reconnectTimeWait: 500,
  json: true
})

nats.on('connect', () => {
  S3og.exitHandler({
    cleanup: () => nats.close(),
    onStop: event => console.log('app stop:', event),
    onDead: error => console.log('app dead:', error)
  })
  pingpong(nats)
  .then(() => sink(nats))
  .then(() => badSubject(nats))
  .then(() => checkLogger(nats))
  .then(() => checkInstance(nats))
  .then(() => process.exit())
  .catch(e => process.exit(e))
})

function hr () {
  // drows delimitter
  console.log(`
******************************TEST******************************
  `)
}
