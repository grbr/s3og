const EventEmitter = require('events')
const path = require('path')
const fs = require('fs')
const promisifyNatsClient = require('./promisify-nats-client')
const stackhash = require('stackhash')
var appDir = path.dirname(require.main.filename)
const { version } = require(path.join(appDir, 'package.json'))

class S3og extends EventEmitter {
  constructor (name, opts) {
    if (!name) throw new Error('name required')
    super()
    this.opts = opts || {}
    this.opts.defaultRequestTimeout = this.opts.defaultRequestTimeout || 30000
    this.opts.maxChainLength = this.opts.maxChainLength || 500
    this.name = name
    this.controllers = []
    this.ether = null
    this.nats = null
    const started = new Date()
    this.metrics = {
      traffic: 0,
      maxLatency: 0,
      averageLatancy: 0,
      totalLatancy: 0,
      _traffic: 0,
      _totalLatancy: 0
    }
    this.use({
      subject: 'instance',
      handler: async () => {
        return {
          name,
          version,
          started,
          memUsedMb: Math.round((process.memoryUsage().heapUsed / 1024 / 1024) * 100) / 100,
          trafficPm: this.metrics.traffic || this.metrics._traffic,
          maxLatencyMs: this.metrics.maxLatency,
          averageLatancyMs: this.metrics.averageLatancy || (this.metrics._totalLatancy / (this.metrics._traffic || 1))
          // totalLatancyMs: this.metrics.totalLatancy || this.metrics._totalLatancy
        }
      }
    })
    setInterval(() => {
      this.metrics.traffic = this.metrics._traffic
      this.metrics._traffic = 0
      this.metrics.averageLatancy = Math.ceil((this.metrics._totalLatancy / (this.metrics.traffic || 1)))
      this.metrics.totalLatancy = this.metrics._totalLatancy
      this.metrics._totalLatancy = 0
    }, 60 * 1000)
  }
  use (controller) {
    const existsWithSameSubject = this.controllers.filter(x => {
      return x.subject === controller.subject
    }).length
    controller.id = `${controller.subject}#${existsWithSameSubject + 1}`
    this.controllers.push(new Controller(controller))
    return this
  }
  go (nats) {
    if (!this.ether) {
      this.nats = promisifyNatsClient(nats, this.opts)
      nats.setDefaultRequestTimeout(this.opts.defaultRequestTimeout)
      this.ether = new Ether(this)
    }
    this.controllers.forEach(controller => {
      const ether = this.ether
      controller.go(nats, async (request, reply, subject) => {
        if (!request) request = {}
        if (!request.meta) {
          request.meta = {
            chain: 'unknown'
          }
        }
        request.meta.subject = subject
        request.meta.reply = reply || null
        const started = (new Date()).getTime()
        const task = new Task(controller, request, subject)
        this.emit('TASK', task)
        const taskResult = await task.go(ether)
        const ended = (new Date()).getTime()
        const latency = ended - started
        this.metrics._traffic++
        this.metrics._totalLatancy += latency
        if (this.metrics.maxLatency < latency) this.metrics.maxLatency = latency
        this.emit('TASK_END', taskResult)
        if (reply) {
          if (taskResult.error) {
            const err = taskResult.error
            ether.error(reply, {
              message: err.message || err.msg || 'unexpected error',
              name: err.name,
              code: err.code || null,
              stackHash: stackhash(err),
              stack: err.stack || null
            })
          } else {
            if (reply) ether.tell(reply, taskResult.result)
          }
        }
      })
    })
    this.emit('LISTEN', this.controllers)
    return this.ether
  }
  stop (reason) {
    this.controllers.forEach(controller => {
      if (controller.subscription) {
        this.nats.unsubscribe(controller.subscription)
        controller.subscription = null
      }
    })
    this.emit('STOP', reason)
  }
  attachLogger (logger) {
    this.on('TASK', ev => {
      logger.info(`[reacts subject]:   ${ev.request.meta.chain}->${ev.id} `)
      logger.child({ ev: 'TASK' }).debug(ev.request)
    })
    this.on('TASK_END', ev => {
      if (ev.error) {
        logger.warn(`[ends with ERR ${ev.took}ms]: ${ev.id}`)
        logger.error(ev.error)
      } else {
        logger.info(`[ends with OK ${ev.took}ms]: ${ev.id}`)
      }
      logger.child({ ev: 'TASK_END' }).debug(ev)
    })
    this.on('STOP', reason => logger.child({ ev: 'APP_STOP' }).warn(`${this.name} stop:`, reason))
    this.on('LISTEN', (controllers) => {
      logger.child({ ev: 'LISTEN' })
      .info(controllers.map(c => `[${c.group}] ${c.subject}`))
    })
    // handling fatal things
    S3og.exitHandler({
      onStop (event) { logger.child({ ev: 'APP_STOP' }).warn(event) },
      onDead (error) { logger.child({ ev: 'APP_STOP' }).fatal(error) }
    })
  }
}

class Controller {
  constructor ({ subject, handler, group, id }) {
    if (!subject) throw new Error('subject required')
    if (!handler) throw new Error('handler required')
    if (!group) group = null
    this.subject = subject
    this.handler = handler
    this.group = group
    this.subscription = null
    this.tasksCount = 0
    this.id = id
  }
  async handle (ether, request, subject) {
    return this.handler(ether, request, subject)
  }
  go (nats, cb) {
    if (!this.subscription) {
      const subscribeOptions = this.group ? { queue: this.group } : {}
      this.subscription = nats.subscribe(this.subject, subscribeOptions, cb)
    }
  }
}

class Task {
  constructor (controller, request, subject) {
    this.controller = controller
    this.request = request
    this.subject = subject
    this.startedAt = (new Date()).getTime()
    this.id = `${controller.id} ${++controller.tasksCount}`
  }
  async go (ether) {
    try {
      const result = await this.controller.handle(ether.child(this), this.request.data, this.subject)
      return new TaskResult(result || null, null, this)
    } catch (error) {
      return new TaskResult(null, error, this)
    }
  }
}

class TaskResult {
  constructor (result, error, task) {
    this.result = result
    this.error = error
    this.id = task.id
    this.subject = task.subject
    this.took = (new Date()).getTime() - task.startedAt
  }
}

const CHAIN_DELIMITER = '->'

class Ether {
  constructor (service, task) {
    this.service = service
    this.nats = service.nats
    if (!task) {
      this.chain = service.name
    } else {
      const maxChainLength = this.service.opts.maxChainLength
      const chain = task.request.meta.chain
      if (chain.length + 4 > maxChainLength) {
        this.chain = chain.substring(0, maxChainLength - 4) + '...' + CHAIN_DELIMITER + task.subject
      } else {
        this.chain = chain + CHAIN_DELIMITER + task.subject
      }
    }
    this.task = task || null
  }
  error (subject, error) {
    this.nats.publish(subject, {
      error: error,
      meta: {
        subject: subject,
        chain: this.chain,
        method: 'error'
      }
    })
  }
  tell (subject, data) {
    this.nats.publish(subject, {
      data: data,
      meta: {
        subject: subject,
        chain: this.chain,
        method: 'tell'
      }
    })
  }
  async ask (subject, data, requestOpts, timeout) {
    try {
      return await this.nats.requestOneAsync(
        subject,
        {
          data: data,
          meta: {
            subject: subject,
            chain: this.chain,
            method: 'ask'
          }
        },
        requestOpts,
        timeout
      )
    } catch (err) {
      if (typeof err === 'object') {
        err.subject = subject
      }
      throw err
    }
  }
  async sink (subject, data, maxTime, max, requestOpts) {
    if (!maxTime) throw new Error('maxTime was not suplied')
    const collected = []
    const errors = []
    return new Promise(async (resolve, reject) => {
      const sid = this.nats.request(
        subject,
        {
          data: data,
          meta: {
            subject: subject,
            chain: this.chain,
            method: 'sink'
          }
        },
        requestOpts,
        response => {
          if (response instanceof Error) errors.push(response)
          if (response.error) {
            errors.push(response.error)
          } else {
            if (collected.length < max || Infinity) collected.push(response.data)
            if (collected.length === max) {
              this.nats.unsubscribe(sid)
              resolve({
                collected: collected,
                errors: errors
              })
            }
          }
        }
      )
      try {
        await asyncTimeout(maxTime)
        resolve({
          collected: collected,
          errors: errors
        })
      } catch (e) { reject(e) }
    })
  }
  child (task) {
    return new Ether(this.service, task)
  }
  die (reason) {
    this.service.stop(reason)
  }
}

S3og.readControllersFromDir = opts => {
  if (!opts.path) throw new Error('path required')
  const mask = opts.mask || /.*.js/
  const rootDir = opts.path.startsWith('/') ? '' : path.dirname(require.main.filename || process.mainModule.filename)
  const baseDir = path.join(rootDir, opts.path)
  const group = opts.group
  return fs.readdirSync(baseDir)
  .filter(f => mask.test(f))
  .map(c => {
    return {
      subject: c.replace(/.js$/, ''),
      handler: require(path.join(baseDir, c)),
      group: group
    }
  })
}

S3og.exitHandler = require('./exit-handler')

function asyncTimeout (to) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve()
    }, to)
  })
}

module.exports = S3og
