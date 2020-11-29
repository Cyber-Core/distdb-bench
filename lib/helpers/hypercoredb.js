const mutexify = require('mutexify')
const hypercore = require('hypercore')

module.exports = HyperCoreDB

function Put (db, key, value, opts, cb) {
    let { batch } = opts

    this._value = '' + key + '#' + value
    this._db = db
    this._release = null

    this._callback = cb
    this._batch = batch

    if (this._batch) this._start()
    else this._lock()
}

Put.prototype._start = function () {
    const self = this
    this._db.ready(function(err){
        self._finalize(err)
    })
}

Put.prototype._lock = function () {
    const self = this
    this._db._lock(function (release) {
        self._release = release
        self._finalize()
    })
}

Put.prototype._finalize = function (err) {
    const self = this

    if (err) return done(err)

    if (this._batch) {
        this._batch.append(this._value)
        return done(null)
    }

    this._db._feed.append(self._value, done)

    function done (err) {
        if (self._release) self._release(self._callback, err)
        else self._callback(err)
    }
}

///////

function Batch (db, ops, cb) {
    this._db = db
    this._ops = ops
    this._callback = cb
    this._values = [ ]
    this._start()
}

Batch.prototype.append = function (value) {
    this._values.push(value)
}

Batch.prototype._finalize = function (err) {
    const self = this
    if (err) return done(err)

    this._db._feed.append(this._values, done)

    function done (err) {
        self._release(self._callback, err, self._values)
    }
}

Batch.prototype._start = function () {
    const self = this
    this._db._lock(function (release) {
        self._release = release
        self._db.ready(function () {
            self._update()
        })
    })
}

Batch.prototype._update = function () {
    let i = 0
    let self = this

    loop(null)

    function loop (err) {
        if (err) return self._finalize(err)
        if (i === self._ops.length) return self._finalize(null)

        const {type, key, value} = self._ops[i++]
        if (type !== 'put') throw 'Wrong type ${type} in batch'

        self._op = new Put(self._db, key, value === undefined ? null : value, { batch: self}, loop)
    }
}

///////////////////

function HyperCoreDB(storage, key, opts) {
    if (!(this instanceof HyperCoreDB)) return new HyperCoreDB(storage, key, opts)

    const feedOpts = Object.assign({}, opts, { valueEncoding: 'binary' })
    this._feed = feedOpts.feed || hypercore(storage, key, feedOpts)
    this._feed.maxRequests = feedOpts.maxRequests || 256 // set max requests higher since the payload is small

    this._lock = mutexify()
}

HyperCoreDB.prototype.ready = function(cb) {
    this._feed.ready(cb)
}

HyperCoreDB.prototype.batch = function(ops, cb) {
    return new Batch(this, ops, cb || noop)
}

HyperCoreDB.prototype.put = function(key, value, opts, cb) {
    if (typeof opts === 'function') return this.put(key, value, null, opts)
    opts = Object.assign({}, opts, {
        batch: null,
        del: 0
    })
    return new Put(this, key, value, opts, cb || noop)
}

HyperCoreDB.prototype.get = function(key, opts, cb) {
    key = Math.round((this._feed.length - 1) * Math.random())
    return this._feed.get(key, opts, cb)
}

HyperCoreDB.prototype.createReadStream = function (prefix, opts) {
    let start = Math.round((this._feed.length - 1) * Math.random())
    let len = 8 // average read for fail searches
    let end = Math.min(this._feed.length, start + len)
    return this._feed.createReadStream({ start: start, end: end })
}