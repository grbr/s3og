module.exports = async (ether, request, subject) => {
  if (request && request.beBad) throw new Error('i\'m bad')
  await asyncTimeout(500) // simulate calculations
  return { randomOne: Math.random() }
}
async function asyncTimeout (ms) {
  return new Promise((resolve, reject) => {
    setTimeout(() => resolve(), ms)
  })
}
