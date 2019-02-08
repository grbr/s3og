const EventEmitter = require('events')
const path = require('path')
const fs = require('fs')
const promisifyNatsClient = require('./promisify-nats-client')
const stackhash = require('stackhash')
const nuid = require('nuid')

class S3og extends EventEmitter {
  constructor (name, opts) {
    if (!name) throw new Error('name required')
    super()
    this.opts = opts || {}
    this.opts.defaultRequestTimeout = this.opts.defaultRequestTimeout || 10000
    this.opts.maxChainLength = this.opts.maxChainLength || 500
    this.name = name
    this.controllers = []
    this.ether = null
    this.nats = null
  }
  use (controller) {
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
        const task = new Task(controller, request, subject)
        this.emit('TASK', task)
        const taskResult = await task.go(ether)
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
}

class Controller {
  constructor ({ subject, handler, group }) {
    if (!subject) throw new Error('subject required')
    if (!handler) throw new Error('handler required')
    if (!group) group = null
    this.subject = subject
    this.handler = handler
    this.group = group
    this.subscription = null
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
    this.id = nuid.next()
  }
  async go (ether) {
    try {
      const result = await this.controller.handle(ether.child(this), this.request.data, this.request.subject)
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
  async ask (subject, data, requestOpts, timeout) {
    return this.nats.requestOneAsync(
      subject,
      {
        data: data,
        meta: {
          subject: subject,
          chain: this.chain
        }
      },
      requestOpts,
      timeout
    )
  }
  async sink (subject, data, maxTime, max, requestOpts) {
    if (!maxTime) throw new Error('maxTime not suplied')
    const collected = []
    const errors = []
    return new Promise(async (resolve, reject) => {
      const sid = this.nats.request(
        subject,
        {
          data: data,
          meta: {
            subject: subject,
            chain: this.chain
          }
        },
        requestOpts,
        response => {
          if (response instanceof Error) errors.push(response)
          if (response.error) {
            errors.push(response.error)
          } else {
            if (collected.length < max) collected.push(response.data)
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
