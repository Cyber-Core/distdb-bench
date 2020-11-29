const random = require('random-seed')

const rand = random.create('hello')

// 10 possible key characters.
const STRING_CHARS = 'abcdefghij'.split('')
const SUBKEY_LEN = 3
const SUBKEY_PARTS = 5
const SUBKEY_SEPARATOR = '/'

module.exports = {
  data: opts => {
    opts = makeDefault(opts)
    let data = new Array(opts.numKeys)
    for (let i = 0; i < opts.numKeys; i++) {
      let key = randomString(opts.subkeyParts, opts.subkeyLength, opts.subkeySeparator)
      data[i] = {
        type: 'put',
        key: key,
        value: rand.string(opts.valueSize),
        schema: 'doc', // for kappa
        id: key // for kappa
      }
    }
    return data
  },
  string: randomString
};

function randomString(subkeyParts, subkeyLength = SUBKEY_LEN, subkeySeparator = SUBKEY_SEPARATOR) {
  let string = ''
  for (let j = 0; j < subkeyParts; j++) {
    if (string.length) string += subkeySeparator
    for (let i = 0; i < subkeyLength; i++) {
      string += STRING_CHARS[rand.intBetween(0, STRING_CHARS.length - 1)]
    }
  }
  return string
}

function makeDefault(opts) {
  return Object.assign({
    valueSize: 10,
    subkeyLength: SUBKEY_LEN,
    subkeyParts: SUBKEY_PARTS,
    subkeySeparator: SUBKEY_SEPARATOR,
    numKeys: 1e3
  }, opts)
}
