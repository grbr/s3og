module.exports = async (ether, request, subject) => {
  console.log(subject)
  ether.tell('test.pingpong.pong', { message: 'pong' })
  return 'pong'
}
