const p = require('path')
const fs = require('fs')
const _lock = require('mutexify')

const lockCreate = new _lock()
const lockRun = new _lock()

const random = require('./lib/helpers/random')
const create = require('./lib/helpers/create')

const STATS_DIR = p.join(__dirname, 'stats')

const readStat = new Map()
const writeStat = new Map()

const READ_SPECS = [
  // { numKeys: 1e1, prefixParts: 2 },
  // { numKeys: 1e2, prefixParts: 2 },
  // { numKeys: 1e3, prefixParts: 2 },
  // { numKeys: 1e4, prefixParts: 2 },
  // { numKeys: 1e5, prefixParts: 10 },
  { numKeys: 1e6, prefixParts: 2 }
]

function makeTag(spec) {
    return Object.keys(spec).map(key => key + ': ' + spec[key]).join(', ')
}

function prepareSpec(src) {
  const dst = Object.assign({
    totalNumKeys: src.numKeys,
    itrNumKeys: 5e4
  }, src)

  dst.subkeyParts = src.prefixParts
  return dst
}

function randomData(spec) {
  const numKeys = Math.min(spec.totalNumKeys, spec.itrNumKeys)
  spec.numKeys = numKeys
  spec.totalNumKeys -= numKeys

  return random.data(spec)
}

function resetStat(info) {
  for (let [name, stat] of info.stat.entries()) {
    stat.readStat.clear()
    stat.writeStat.clear()
  }
}

function dumpReadStat(type, info) {
  console.log(type)
  let total_cnt = 0
  let total_size = 0
  for (let [name, stat] of info.stat.entries()) {
    for (let [size, cnt] of stat.readStat.entries()) {
      total_cnt += cnt
      total_size += cnt * size
      console.log(' read ' + name + ' ' + size + ' ' + cnt)
    }
  }
  console.log(` total-read-count ${total_cnt} total-read-size ${total_size}`)
  console.log('\n')
}

function dumpFullStat(type, info) {
  console.log(type)
  let total_read_cnt = 0
  let total_read_size = 0
  let total_write_cnt = 0
  let total_write_size = 0
  for (let [name, stat] of info.stat.entries()) {
    for (let [size, cnt] of stat.writeStat.entries()) {
      total_write_cnt += cnt
      total_write_size += cnt * size
      console.log(' write ' + name + ' ' + size + ' ' + cnt)
    }
    for (let [size, cnt] of stat.readStat.entries()) {
      total_read_cnt += cnt
      total_read_size += cnt * size
      console.log(' read ' + name + ' ' + size + ' ' + cnt)
    }
  }
  console.log(` total-write-count ${total_write_cnt} total-write-size ${total_write_size}`)
  console.log(` total-read-count ${total_read_cnt} total-read-size ${total_read_size}`)
  console.log('\n')
}

function runner(tag, info, done) {
  let spec = prepareSpec(tag)
  let existKeys = []

  function _setup() {
    const data = randomData(spec)
    existKeys.push(data[0].key)

    if (info.db.batch) {
      info.db.batch(data, function (err) {
        if (err) return done(err)
        if (spec.totalNumKeys !== 0) return _setup()

        _singleFailRead()
      })
    } else {
      function _singleInsert() {
        const item = data.pop()
        info.db.put(item.key, item.value, function (err) {
          if (err) done(err)
          else if (data.length !== 0 ) _singleInsert()
          else if (spec.totalNumKeys !== 0) _setup()
          else _singleFailRead()
        })
      }

      _singleInsert()
    }
  }

  function _singleFailRead() {
    resetStat(info)

    let waitKey = existKeys.length
    for (let key of existKeys) {
      info.db.get('aaa' + key , {}, function (err) {
        if (-- waitKey !== 0) return

        dumpReadStat(`${existKeys.length} single fails`, info)
        _singleGoodRead()
      })
    }
  }

  function _singleGoodRead() {
    resetStat(info)

    let waitKey = existKeys.length
    for (let key of existKeys) {
      info.db.get(key, {}, function (err) {
        if (-- waitKey !== 0) return

        dumpReadStat(`${existKeys.length} single goods`, info)
        _singleInsert()
      })
    }
  }

  function _singleInsert() {
    resetStat(info)

    let waitKey = existKeys.length
    let value = ''.padEnd(10, '0')
    for (let key of existKeys) {
      info.db.put('aaa' + key, value, function (err) {
        if (err) return done(err)
        if (-- waitKey !== 0) return

        dumpFullStat(`${existKeys.length} single inserts`, info)
        _singleDelete()
      })
    }
  }

  function _singleDelete() {
    resetStat(info)

    if (!info.db.del) return done(null)

    let waitKey = existKeys.length
    for (let key of existKeys) {
      info.db.del(key, function (err) {
        if (-- waitKey !== 0) return

        dumpFullStat(`${existKeys.length} single deletes`, info)
        done(null)
      })
    }
  }

  console.log(`# ${info.name} with ` + makeTag(tag))
  _setup()
}

function run() {

  function _run(spec, info, done) {
    lockRun(releaseRun => {
      runner(spec, info, err => {
        if (err) throw err
        console.log(`ok ~ ${info.name}\n`)
        releaseRun()
        done()
      })
    })
  }

  for (let spec of READ_SPECS) {
    lockCreate(releaseCreate => {
      create({ withStats: true }, (err, dbs) => {
        if (err) throw err

        let waitDBs = 1
        for (let info of dbs) {
          if (info.stat) {
            waitDBs ++
            _run(spec, info, _done)
          }
        }

        function _done() {
          if (-- waitDBs) releaseCreate()
        }

        _done()

      })
    })
  }

}
run()

process.on('exit', () => {
    const d = new Date();
    const dt = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}_${pad(d.getHours())}-${pad(d.getMinutes())}`;
    saveStats(dt)

    function pad(x) {
        return (x < 10 ? '0' : '') + x
    }

    function saveStats(dt) {
        // let csv = 'type,db,numKeys,numReads,readLength,prefixLength,'
        //
        // fs.mkdirSync(STATS_DIR, {recursive: true})
        // let statFile = p.join(STATS_DIR, `${dt}_access.csv`)
        // fs.writeFileSync(statFile, csv, { valueEncoding: 'utf8' })
    }
})