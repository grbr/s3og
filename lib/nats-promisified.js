const NATS = require('nats')

NATS.promisifyClient = (nats, config) => {
  if (config.json) {
    /*
      JSON
    */
    nats.requestAsync = (subject, request, opts) => new Promise((resolve, reject) => {
      try {
        if (opts) {
          opts.max = 1
        } else {
          opts = {max: 1}
        }
        nats.request(subject, request, opts, (response) => {
          if (response instanceof NATS.NatsError) {
            reject(response)
          } else {
            if (response.error) {
              reject(response.error)
            } else {
              resolve(response.data)
            }
          }
        })
      } catch (e) {
        reject(e)
      }
    })
    nats.requestOneAsync = (subject, request, opts, timeout) => new Promise((resolve, reject) => {
      try {
        nats.requestOne(subject, request, opts, timeout || nats.$defaultRequestTimeout, (response) => {
          if (response instanceof NATS.NatsError) {
            reject(response)
          } else {
            if (response.error) {
              reject(response.error)
            } else {
              resolve(response.data)
            }
          }
        })
      } catch (e) {
        reject(e)
      }
    })
  } else {
    /*
      RAW
    */
    nats.requestAsync = (subject, request, opts) => new Promise((resolve, reject) => {
      try {
        if (opts) {
          opts.max = 1
        } else {
          opts = {max: 1}
        }
        nats.request(subject, request, opts, (response) => {
          if (response instanceof NATS.NatsError) {
            reject(response)
          } else {
            resolve(response)
          }
        })
      } catch (e) {
        reject(e)
      }
    })
    nats.requestOneAsync = (subject, request, opts, timeout) => new Promise((resolve, reject) => {
      try {
        nats.requestOne(subject, request, opts, timeout || nats.$defaultRequestTimeout, (response) => {
          if (response instanceof NATS.NatsError) {
            reject(response)
          } else {
            resolve(response)
          }
        })
      } catch (e) {
        reject(e)
      }
    })
  }
  nats.ready = new Promise((resolve, reject) => {
    nats
    .on('connect', (nc) => {
      resolve(nc)
    })
    .on('error', (err) => {
      reject(err)
    })
  })
  nats.setDefaultRequestTimeout = (timeout) => {
    if (timeout) {
      nats.$defaultRequestTimeout = timeout
    }
  }
  return nats
}

NATS.connectAsync = config => {
  const nats = NATS.promisifyClient(NATS.connect(config), config)
  return nats.ready
}

module.exports = NATS
