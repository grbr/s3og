let counter = 10
module.exports = async (proxy, request, subject) => {
  if (--counter > 0) {
    proxy.tell('test.pingpong.ping', { message: 'ping ' + counter })
    return 'ping'
  } else {
    console.log('end', new Date())
    proxy.tell('test.pingpong.poisonpill')
    return 'end'
  }
}
