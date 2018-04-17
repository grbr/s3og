module.exports = async (proxy, request, subject) => {
  proxy.die('poison pill')
}
