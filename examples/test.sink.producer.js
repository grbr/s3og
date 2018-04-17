module.exports = async (proxy, request, subject) => {
  return { randomOne: Math.random() }
}
