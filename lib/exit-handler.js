const stackhash = require('stackhash')

let exitHandler = null
let listen = false

// handle critical errors and application stop
module.exports = (opts) => {
  opts = opts || {}
  const {cleanup, onStop, onDead} = opts
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
    process.on('exit', () => exitHandler({cleanup: true, event: 'exit'}))
    process.on('SIGINT', () => exitHandler({exit: true, event: 'SIGINT'}))
    process.on('SIGUSR1', () => exitHandler({exit: true, event: 'SIGUSR1'}))
    process.on('SIGUSR2', () => exitHandler({exit: true, event: 'SIGUSR2'}))
    process.on('uncaughtException', () => exitHandler({exit: true, event: 'uncaughtException'}))
    listen = true
  }
}
