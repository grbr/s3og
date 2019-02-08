const stackhash = require('stackhash')

let exitHandler = null
let listen = false

// handle critical errors and application stop
module.exports = (opts) => {
  opts = opts || {}
  const { cleanup, onStop, onDead } = opts
  ;[cleanup, onStop, onDead].forEach(act => {
    if (act && typeof act !== 'function') throw new Error('handler must be function if defined (cleanup, onStop, onDead)')
  })
  const cleanupAction = cleanup || (() => {})
  const onStopAction = onStop || (event => { console.log('APP_STOP', event) })
  const onDeadAction = onDead || (error => { console.log('APP_DEAD', error) })
  exitHandler = (options, err) => {
    if (err) err.stackhash = stackhash(err)
    if (options.cleanup) {
      cleanupAction()
      onStopAction(options.event)
    }
    if (err) onDeadAction(err)
    if (options.exit) process.exit()
  }
  if (!listen) {
    process.on('exit', (ev) => exitHandler({ cleanup: true, event: 'exit' }, ev))
    process.on('SIGINT', (ev) => exitHandler({ exit: true, event: 'SIGINT' }, ev))
    process.on('SIGUSR1', (ev) => exitHandler({ exit: true, event: 'SIGUSR1' }, ev))
    process.on('SIGUSR2', (ev) => exitHandler({ exit: true, event: 'SIGUSR2' }, ev))
    process.on('uncaughtException', (ev) => exitHandler({ exit: true, event: 'uncaughtException' }, ev))
    listen = true
  }
}
