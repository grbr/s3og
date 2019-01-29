let counter = 10
module.exports = async (ether, request, subject) => {
  if (--counter > 0) {
    ether.tell('test.pingpong.ping', { message: 'ping ' + counter })
    return 'ping'
  } else {
    console.log('end', new Date())
    ether.tell('test.pingpong.poisonpill')
    return 'end'
  }
}
