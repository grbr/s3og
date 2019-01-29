module.exports = async (ether, request, subject) => {
  ether.tell('test.pingpong.pong', { message: 'pong' })
  return 'pong'
}
