const p = require('path')
const fs = require('fs');
const os = require('os')

const leveldb = require('level')
const memleveldb = require('memdb')
const hyperdb = require('hyperdb')
const hypertrie = require('hypertrie')
const hypercore = require('./hypercoredb')
const hyperdrive = require('./hyperdrivedb')
const kappa = require('./kappadb')
const ram = require('random-access-memory')
const RandomAccessFile = require('random-access-file')
const RandomAccessWithStat = require('./random-access-with-stat')


let count = 0
let dir = p.join(os.tmpdir(), (Math.abs(Math.random() * 0xFFFFFFFF | 0)).toString())
fs.mkdirSync(dir, {recursive: true});
console.log(dir);

module.exports = (globalOpts, cb) => {
  let dbs = [ ]

  function _replica(dbCtor, info) {
    return function(done) {
      const replica = dbCtor(FileNoStat(), info.db.key)
      replica.ready(done)
      return replica
    }
  }

  function _createInRAM(dbName, dbCtor) {
    const info = {
      name: dbName,
      db: dbCtor(ram),
      inRAM: true
    }
    // info.replica = _replica(dbCtor, info)
    dbs.push(info)
  }

  function _create(dbName, dbCtor, opts = globalOpts) {
    let info = {
      name: dbName,
      inRAM: false
    }

    if (opts.withStats) {
      info.stat = new Map()
      info.db = dbCtor(FileWithStat(info.stat))
    } else {
      info.db = dbCtor(FileNoStat())
    }

    info.replica = _replica(dbCtor, info)

    dbs.push(info)
  }

  dbs.push({ name: 'leveldb', db: leveldb(FileNoStat()), inRAM: false })
  dbs.push({ name: 'leveldb in mem', db: memleveldb(), inRAM: true })

  _create('kappa', kappa, {})
  _createInRAM('kappa in RAM', kappa)

  _create('hypercore', hypercore)
  _createInRAM('hypercore in mem', hypercore)

  _create('hypertrie', hypertrie)
  _createInRAM('hypertrie in mem', hypertrie)

  _create('hyperdb', hyperdb)
  _createInRAM('hyperdb in mem', hyperdb)

  _create('hyperdrive', hyperdrive)
  // // _createInRAM('hyperdrive in mem', hyperdrive)

  let wait = 1
  for (const info of dbs) {
    if (info.db.ready) {
      wait++
      info.db.ready(ready)
    }
  }

  ready(null)

  function ready(err) {
    if (err) return cb(err)

    --wait;
    if (wait === 0) cb(null, dbs)
  }
}

process.on('exit', () => {
  fs.rmdirSync(dir, {recursive: true});
})

function FileWithStat(stat) {
  const c = count ++
  return function (name, opts) {
    stat.set(name, { })
    return RandomAccessWithStat(stat.get(name), function () {
      try {
        var lock = (name === 'bitfield' || name.endsWith('/bitfield')) ? require('fd-lock') : null
      } catch (err) {}

      return RandomAccessFile(name, { directory: p.join(dir, '' + c), lock: lock })
    })
  }
}

function FileNoStat() {
  const c = count ++
  return p.join(dir, '' + c)
}