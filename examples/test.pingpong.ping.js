module.exports = async (proxy, request, subject) => {
  proxy.tell('test.pingpong.pong', { message: 'pong' })
  return 'pong'
}
