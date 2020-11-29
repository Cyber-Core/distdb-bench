const p = require('path')
const fs = require('fs')
const events = require('events')
const inherits = require('inherits')
const lock = require('mutexify')()

const random = require('./lib/helpers/random')
const bench = require('./lib/helpers/bench')
const create = require('./lib/helpers/create')

const STATS_DIR = p.join(__dirname, 'stats')
const TRIALS = 5
const WRITE_SPECS = [
  // { numKeys: 1e1 },
  // { numKeys: 1e2 },
  // { numKeys: 1e3 },
  // { numKeys: 1e4 },
  // { numKeys: 1e5 },
  { numKeys: 1e6 }
]
const READ_SPECS = [
  // { numKeys: 1e1, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e2, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e3, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e4, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e5, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e1, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e2, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e3, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e4, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e5, numReads: 1e3, readLength: 30, prefixParts: 1 },
  // { numKeys: 1e1, numReads: 1e3, readLength: 30, prefixParts: 2 },
  // { numKeys: 1e2, numReads: 1e3, readLength: 30, prefixParts: 2 },
  // { numKeys: 1e3, numReads: 1e3, readLength: 30, prefixParts: 2 },
  // { numKeys: 1e4, numReads: 1e3, readLength: 30, prefixParts: 2 },
  // { numKeys: 1e5, numReads: 1e3, readLength: 30, prefixParts: 2 },
  { numKeys: 1e6, numReads: 1e4, readLength: 30, prefixParts: 2 }
]

let writeStats = []
let readStats = []

function convertToNs (time) {
  return 1e9 * time[0] + time[1]
}

function makeTag(spec) {
  return Object.keys(spec).map(key => key + ': ' + spec[key]).join(', ')
}

function prepareSpec(src) {
  const dst = Object.assign({
    totalNumKeys: src.numKeys,
    itrNumKeys: 1e5
  }, src)

  if (src.prefixParts) dst.subkeyParts = src.prefixParts + 1

  return dst
}

function Benchmarker(tag, cb) {
  if (!(this instanceof Benchmarker)) return new Benchmarker(tag, cb)

  const self = this
  let _remDBs = 0
  let _remTrials = TRIALS
  let _stats = [ ]

  for (let i = 0; i < TRIALS; i++) {
    lock(function (release) {
      create({}, (err, dbs) => {
        if (err) throw err

        if (_stats.length === 0) {
          for (let db of dbs) _stats.push({name: db.name, results: []})
        }

        function doBench(info, stat) {
          bench(`${tag} with ${stat.name}`, b => {
            b.done = () => {
              if (!b.skipped) stat.results.push(convertToNs(b.time))

              if (--_remDBs === 0) {
                if (-- _remTrials === 0) {
                  self.emit('finish', _stats)
                }
                release()
              }
            }
            return cb(b, info)
          })
        }

        _remDBs += dbs.length
        for (let j = 0; j < dbs.length; j++) {
          doBench(dbs[j], _stats[j])
        }
      })
    })
  }

  return this
}

inherits(Benchmarker, events.EventEmitter)

function runner(specs, stats, tag, func) {
  specs.forEach(spec => {
    const benchmark = Benchmarker([tag, makeTag(spec)].join(' '), (b, info) => {
      b.start()
      func(prepareSpec(spec), b, info, function (err) {
        if (err) throw err
        b.end()
      })
    })
    benchmark.on('finish', times => {
      stats.push({
        type: tag,
        spec: spec,
        timing: times
      })
    })
  })
}

function randomData(spec) {
  const numKeys = Math.min(spec.totalNumKeys, spec.itrNumKeys)
  spec.numKeys = numKeys
  spec.totalNumKeys -= numKeys

  return random.data(spec)
}

function writeRunner(type, checker, func) {
  return runner(WRITE_SPECS, writeStats, type, function(spec, b, info, done) {
    if (!checker(info)) {
      b.skip()
      return done(null)
    }

    function _insert() {
      b.pause()
      const data = randomData(spec)
      b.resume()

      func(b, info, data, function (err) {
        if (err || spec.totalNumKeys === 0) return done(err)

        _insert()
      })
    }

    _insert()
  })
}

function benchBatchInsertions() {
  function checker(info) {
    return info.db.batch
  }

  writeRunner('batch write', function (b, info, data, done) {
    info.db.batch(data, done)
  })
}

function benchBigBatchInsertions() {
  return runner(WRITE_SPECS, writeStats, 'batch big write', function(spec, b, info, done) {
    if (!info.db.batch || info.inRAM) {
      b.skip()
      return done(null)
    }

    function _insert() {
      b.pause()
      const data = randomData(spec)
      b.resume()

      info.db.batch(data, function(err) {
        if (err || spec.totalNumKeys === 0) return done(err)
        _insert()
      })
    }

    spec.valueSize = 1024
    _insert()
  })
}

function benchSingleInsertions() {
  function checker(info) {
    return true
  }

  writeRunner('single write', checker, function (b, info, data, done) {
    function _insert() {
      const item = data.pop()

      info.db.put(item.key, item.value, function (err) {
        if (err || data.length === 0) return done(err)

        _insert()
      })
    }

    _insert()
  })
}

function readRunner(type, checker, func) {
  return runner(READ_SPECS, readStats, type, function(spec, b, info, done) {
    if (!checker(info)) {
      b.skip()
      return done(null)
    }

    function _setup(err) {
      if (err) return done(err)

      if (spec.totalNumKeys === 0)  {
        b.resume()
        return func(spec, b, info, done)
      }

      let data = randomData(spec)
      if (info.db.batch) {
        info.db.batch(data, function (err) {
          _setup(err)
        })
      } else {
        function _singleInsert() {
          const item = data.pop()
          info.db.put(item.key, item.value, function (err) {
            if (err || data.length === 0) _setup(err)
            else _singleInsert()
          })
        }

        _singleInsert()
      }
    }

    b.pause()
    _setup()
  })
}

function benchBatchRandomReads() {
  function checker(info) {
    return info.db.createReadStream
  }

  readRunner('random batch reads', checker, function(spec, b, info, done) {

    function _read(prefix) {
      let counter = 0
      const stream = info.db.createReadStream(prefix, {forcedLength: spec.readLength})
      stream.on('data', d => {
        if (counter++ === spec.readLength) {
          stream.destroy()
          stream.on('close', _reader)
        }
      })
      stream.on('end', _reader)
    }

    let totalCounter = 0
    function _reader() {
      if (totalCounter++ > spec.numReads) return done(null)
      _read(random.string(spec.prefixParts))
    }

    _reader()
  })
}

function benchSingleRandomReads() {
  function checker() {
    return true
  }

  readRunner('random single reads', checker, function(spec, b, info, done) {
    let counter = 0

    function _read() {
      const key = random.string(spec.prefixParts)
      info.db.get(key, {}, function (err) {
        if (counter++ > spec.numReads * spec.readLength) return done(null)
        _read()
      })
    }

    _read()
  })
}

function benchReplicaSync() {
  function checker(info) {
    return info.name === 'hypertrie'
  }

  readRunner('replica sync', checker, function(spec, b, info, done) {
    b.pause()
    const replica = info.replica(function (err) {
      if (err) return done(err)
      b.resume()
      _replicate()
    })

    function _replicate() {
      const masterStream = info.db.replicate({initiator: true, live: true})
      const replicaStream = replica.replicate({initiator: false, live: true})
      let remaining = info.db.feed.length - 1
      masterStream.pipe(replicaStream).pipe(masterStream)
      
      function _read() {
        replica.feed.get(remaining, {}, function(err){
          if (-- remaining !== -1) return _read()
          done(null)
        })
      }

      function wait () {
        if (remaining !== -1) setTimeout(wait, 1000);
      }

      _read()
      wait()
    }
  })
}

function run() {
  benchBatchInsertions()
  benchSingleInsertions()
  // benchBatchRandomReads()
  benchBigBatchInsertions()
  benchSingleRandomReads()
  benchReplicaSync()
}
run()

process.on('exit', () => {
  const d = new Date();
  const dt = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}_${pad(d.getHours())}-${pad(d.getMinutes())}`;
  saveReadStats(dt)
  saveWriteStats(dt)

  function pad(x) {
    return (x < 10 ? '0' : '') + x
  }

  function saveWriteStats(dt) {
    let csv = 'type,db,numKeys,'
    for (let i = 0; i < TRIALS; i++) {
      csv += 't' + i + ','
    }
    csv += '\n'
    for (i = 0; i < writeStats.length; i++) {
      let stat = writeStats[i]
      for (let timing of stat.timing) {
        if (timing.results.length === 0) continue
        csv += [stat.type, timing.name, stat.spec.numKeys].join(',') + ','
        csv += timing.results.join(',')
        csv += '\n'
      }
    }
    fs.mkdirSync(STATS_DIR, {recursive: true})
    let statFile = p.join(STATS_DIR, `${dt}_writes-random-data.csv`)
    fs.writeFileSync(statFile, csv, { valueEncoding: 'utf8' })
  }
  function saveReadStats(dt) {
    let csv = 'type,db,numKeys,numReads,readLength,prefixLength,'
    for (let i = 0; i < TRIALS; i++) {
      csv += 't' + i + ','
    }
    csv += '\n'
    for (i = 0; i < readStats.length; i++) {
      let stat = readStats[i]
      for (let timing of stat.timing) {
        if (timing.results.length === 0) continue
        csv += [stat.type, timing.name, stat.spec.numKeys, stat.spec.numReads,
          stat.spec.readLength,
          stat.spec.prefixParts * 3 + (stat.spec.prefixParts - 1)].join(',') + ','
        csv += timing.results.join(',')
        csv += '\n'
      }
    }
    fs.mkdirSync(STATS_DIR, {recursive: true})
    let statFile = p.join(STATS_DIR, `${dt}_reads-random-data.csv`)
    fs.writeFileSync(statFile, csv, { valueEncoding: 'utf8' })
  }
})


