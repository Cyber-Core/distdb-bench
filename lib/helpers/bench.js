const lock = require('mutexify')()
const prettyHrtime = require('pretty-hrtime')

let _cur = null
let _runs = 0
let _total = [0, 0]

module.exports = benchmark

function convertToNs (time) {
  return 1e9 * time[0] + time[1]
}

function rawTime (hr) {
  return '(' + hr[0] + ' s + ' + hr[1] + ' ns, ' + convertToNs(hr) + ' ns)'
}

function addTime(total, time) {
  total[0] += time[0]
  total[1] += time[1]
  while (total[1] >= 1e9) {
    total[1] -= 1e9
    total[0]++
  }
}

function subTime(total, time) {
  total[0] -= time[0]
  total[1] -= time[1]
  while (total[1] < 0) {
    total[1] += 1e9
    total[0]--
  }
}

function benchmark(name, fn) {
  process.nextTick(function () {
    _runs++
    lock(function (release) {
      console.log('# ' + name)

      let _b = _cur = {}
      let _begin = process.hrtime()
      let _pause = [0, 0]
      let _pauseCnt = 0
      let _pauseTime = [0, 0]

      _b.start = function () {
        _begin = process.hrtime()
        _pause = []
      }

      _b.skipped = false

      _b.skip = function () {
        _b.skipped = true
      }

      _b.pause = function () {
        _pauseCnt++
        if (_pauseCnt > 1) return // already paused
        _pause = process.hrtime()
      }

      _b.resume = function () {
        _pauseCnt--
        if (_pauseCnt < 0) throw 'resume() called more times then pause()'
        if (_pauseCnt > 0) return // still paused

        addTime(_pauseTime, process.hrtime())
        subTime(_pauseTime, _pause)
      }

      _b.error = function (err) {
        _cur = null
        console.log('fail ' + err.message + '\n')
      }

      _b.log = function (msg) {
        console.log('# ' + msg)
      }

      _b.end = function (msg) {
        if (msg) _b.log(msg)

        if (_pauseCnt !== 0) {
          _pauseCnt = 1
          _b.resume()
        }

        _cur = null

        let _time = process.hrtime()
        subTime(_time, _begin)
        subTime(_time, _pauseTime)

        addTime(_total, _time)

        if (_b.skipped)
          console.log('skipped\n')
        else if (_pauseTime[0] === 0 && _pauseTime[1] === 0)
          console.log('ok ~' + prettyHrtime(_time) + ' ' + rawTime(_time) + '\n')
        else
          console.log('ok ~' + prettyHrtime(_time) + ' ' + rawTime(_time) + ' + pause ~' + prettyHrtime(_pauseTime) + ' ' + rawTime(_pauseTime) + '\n')

        _b.time = _time

        if (_b.done) _b.done()
        release()
      }

      fn(_b)
    })
  })
}

process.on('exit', function () {
  if (_cur) {
    _cur.error(new Error('bench was never ended'))
    console.log('fail\n')
    return
  }
  console.log('all benchmarks completed')
  console.log('ok ~' + prettyHrtime(_total) + ' ' + rawTime(_total) + '\n')
})
