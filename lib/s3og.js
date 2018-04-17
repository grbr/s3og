const EventEmitter = require('events')
const path = require('path')
const fs = require('fs')
const NatsPromisified = require('./nats-promisified')
const NATS = require('nats')
const stackhash = require('stackhash')
const nuid = require('nuid')

class M3Service extends EventEmitter {
  constructor (name, opts) {
    if (!name) throw new Error('name required')
    super()
    this.opts = opts || {}
    this.opts.defaultRequestTimeout = this.opts.defaultRequestTimeout || 10000
    this.opts.maxChainLength = this.opts.maxChainLength || 500
    this.name = name
    this.controllers = []
    this.proxy = null
    this.nats = null
  }
  use (controller) {
    const c = new Controller(controller)
    this.controllers.push(c)
    return this
  }
  go (nats) {
    if (!this.proxy) {
      this.nats = NatsPromisified.promisifyClient(nats, {json: true})
      nats.setDefaultRequestTimeout(this.opts.defaultRequestTimeout)
      this.proxy = new Proxy(this)
    }
    this.controllers.forEach(controller => {
      const proxy = this.proxy
      controller.go(nats, async (request, reply, subject) => {
        if (!request) request = {}
        if (!request.meta) {
          request.meta = {
            chain: 'unknown'
          }
        }
        request.meta.subject = subject
        request.meta.reply = reply || null
        const task = new Task(controller, request, subject)
        this.emit('TASK', task)
        const taskResult = await task.go(proxy)
        this.emit('TASK_END', taskResult)
        if (reply) {
          if (taskResult.error) {
            const err = taskResult.error
            proxy.error(reply, {
              message: err.message || err.msg || 'unexpected error',
              name: err.name,
              code: err.code || null,
              stackHash: stackhash(err),
              stack: err.stack || null
            })
          } else {
            if (reply) proxy.tell(reply, taskResult.result)
          }
        }
      })
    })
    return this.proxy
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
}

class Controller {
  constructor ({subject, handler, group}) {
    if (!subject) throw new Error('subject required')
    if (!handler) throw new Error('handler required')
    if (!group) group = null
    this.subject = subject
    this.handler = handler
    this.group = group
    this.subscription = null
  }
  async handle (proxy, request, subject) {
    return this.handler(proxy, request, subject)
  }
  go (nats, cb) {
    if (!this.subscription) {
      const subscribeOptions = this.group ? {queue: this.group} : {}
      this.subscription = nats.subscribe(this.subject, subscribeOptions, cb)
    }
  }
}

class Task {
  constructor (controller, request, subject) {
    this.controller = controller
    this.request = request
    this.subject = subject
    this.id = nuid.next()
  }
  async go (proxy) {
    try {
      const result = await this.controller.handle(proxy.child(this), this.request.data, this.request.subject)
      return new TaskResult(result || null, null, this.id)
    } catch (error) {
      return new TaskResult(null, error, this)
    }
  }
}

class TaskResult {
  constructor (result, error, task) {
    this.result = result
    this.error = error
    this.task = task
  }
}

const CHAIN_DELIMITER = '->'

class Proxy {
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
        chain: this.chain
      }
    })
  }
  tell (subject, data) {
    this.nats.publish(subject, {
      data: data,
      meta: {
        subject: subject,
        chain: this.chain
      }
    })
  }
  async ask (subject, data, opts, timeout) {
    return this.nats.requestOneAsync(subject, {
      data: data,
      meta: {
        subject: subject,
        chain: this.chain
      },
      opts,
      timeout
    })
  }
  async sink (subject, data, maxTime, max, opts) {
    if (!maxTime) throw new Error('maxTime not suplied')
    const collected = []
    const errors = []
    return new Promise(async (resolve, reject) => {
      this.nats.request(
        subject,
        {
          data: data,
          meta: {
            subject: subject,
            chain: this.chain
          }
        },
        opts,
        response => {
          if (response instanceof NATS.NatsError) errors.push(response)
          if (response.error) {
            errors.push(response.error)
          } else {
            collected.push(response.data)
            if (collected.length === max) {
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
    return new Proxy(this.service, task)
  }
  die (reason) {
    this.service.stop(reason)
  }
}

M3Service.readControllersFromDir = opts => {
  if (!opts.path) throw new Error('path required')
  const mask = opts.mask || /.*.js/
  const baseDir = path.join(path.dirname(require.main.filename || process.mainModule.filename), opts.path)
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

M3Service.exitHandler = require('./exit-handler')

function asyncTimeout (to) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve()
    }, to)
  })
}

module.exports = M3Service
