const EventEmitter = require('events')
const path = require('path')
const fs = require('fs')
const promisifyNatsClient = require('./promisify-nats-client')
const stackhash = require('stackhash')
const { customAlphabet } = require('nanoid')
var appDir = path.dirname(require.main.filename)
const { version, name } = require(path.join(appDir, 'package.json'))
const packageName = name

const S3OG_INSTANCE_NAME = process.env.S3OG_INSTANCE_NAME
const S3OG_SERVICE_NAME = process.env.S3OG_SERVICE_NAME
const S3OG_NANOID_ALPHABET = process.env.S3OG_NANOID_ALPHABET || '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
const S3OG_NANOID_LENGTH = process.env.S3OG_NANOID_LENGTH || 5
const S3OG_METRICS_RECALC_INTERVAL_SECONDS = process.env.S3OG_METRICS_RECALC_INTERVAL_SECONDS || +60
const S3OG_DEFAULT_TO = process.env.S3OG_DEFAULT_TO
const S3OG_MAX_CHAIN_LENGTH = process.env.S3OG_MAX_CHAIN_LENGTH

const nanoid = customAlphabet(S3OG_NANOID_ALPHABET, S3OG_NANOID_LENGTH)

class S3og extends EventEmitter {
  constructor (name, opts) {
    super()
    // env > hardcoded > package.json
    this.serviceName = S3OG_SERVICE_NAME || name || packageName
    // env or <serviceName>-<short random>
    this.name = S3OG_INSTANCE_NAME || `${this.serviceName}-${nanoid()}`
    this.opts = opts || {}
    this.opts.defaultRequestTimeout = S3OG_DEFAULT_TO || this.opts.defaultRequestTimeout || 30000
    this.opts.maxChainLength = S3OG_MAX_CHAIN_LENGTH || this.opts.maxChainLength || 500
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
      _totalLatancy: 0,
      _maxLatency: 0
    }
    this.use({
      subject: 'instance',
      handler: async () => {
        return {
          name: this.name,
          service: this.serviceName,
          version,
          started,
          memUsedMb: Math.round((process.memoryUsage().heapUsed / 1024 / 1024) * 100) / 100,
          trafficPm: this.metrics.traffic || this.metrics._traffic,
          maxLatencyMs: (this.metrics.maxLatency || this.metrics._maxLatency),
          averageLatancyMs: Math.ceil(this.metrics.averageLatancy || (this.metrics._totalLatancy / (this.metrics._traffic || 1)))
        }
      }
    })
    setInterval(() => {
      this.metrics.traffic = this.metrics._traffic
      this.metrics._traffic = 0
      this.metrics.averageLatancy = Math.ceil((this.metrics._totalLatancy / (this.metrics.traffic || 1)))
      this.metrics.maxLatency = this.metrics._maxLatency
      this.metrics._maxLatency = 0
      this.metrics.totalLatancy = this.metrics._totalLatancy
      this.metrics._totalLatancy = 0
    }, S3OG_METRICS_RECALC_INTERVAL_SECONDS * 1000)
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
        if (this.metrics._maxLatency < latency) this.metrics._maxLatency = latency
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
    this.on('TASK', task => {
      if (task.subject !== 'instance') {
        logger.debug(`[reacts subject]:   ${task.request.meta.chain}->${task.id} `)
        logger.child({ ev: 'TASK' }).debug(task.request)
      }
    })
    this.on('TASK_END', taskResult => {
      if (taskResult.error) {
        logger.warn(`[ends with ERR ${taskResult.took}ms]: ${taskResult.id}`)
        logger.error(taskResult.error)
        logger.child({ ev: 'TASK_END' }).debug(taskResult)
      } else {
        if (taskResult.subject !== 'instance') {
          logger.info(`[ends with OK ${taskResult.took}ms]: ${taskResult.id}`)
          logger.child({ ev: 'TASK_END' }).debug(taskResult)
        }
      }
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
    return new Promise((resolve, reject) => {
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
      asyncTimeout(maxTime)
      .then(() => {
        resolve({
          collected: collected,
          errors: errors
        })
      })
      .catch(reject)
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
