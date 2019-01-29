module.exports = (nats, promisificationOptions) => {
  nats.$defaultRequestTimeout = promisificationOptions.defaultRequestTimeout || 10000
  nats.requestAsync = (subject, request, opts) => new Promise((resolve, reject) => {
    try {
      if (opts) {
        opts.max = 1
      } else {
        opts = { max: 1 }
      }
      nats.request(subject, request, opts, (response) => {
        if (response instanceof Error) {
          reject(response)
        } else {
          if (response.error) {
            reject(response.error)
          } else {
            resolve(response.data)
          }
        }
      })
    } catch (err) {
      reject(err)
    }
  })
  nats.requestOneAsync = (subject, request, opts, timeout) => new Promise((resolve, reject) => {
    try {
      nats.requestOne(subject, request, opts, timeout || nats.$defaultRequestTimeout, (response) => {
        if (response instanceof Error) {
          reject(response)
        } else {
          if (response.error) {
            reject(response.error)
          } else {
            resolve(response.data)
          }
        }
      })
    } catch (err) {
      reject(err)
    }
  })
  nats.setDefaultRequestTimeout = (timeout) => {
    if (timeout) {
      nats.$defaultRequestTimeout = timeout
    }
  }
  return nats
}
