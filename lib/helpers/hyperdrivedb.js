const mutexify = require('mutexify')
const hyperdrive = require('hyperdrive')
const steams = require('streamx')

module.exports = HyperDriveDB

class HDriveReadStream extends steams.Readable {
    constructor (drive, prefix) {
        super()

        this.drive = drive
        this.prefix = prefix
        this.items = []
    }

    _open (cb) {
        const self = this
        this.drive.readdir(this.prefix, function(err, items) {
            if (err) return cb(err)
            for (let item of items) self.items.push(self.prefix + '/' + item)
            cb(null)
        })
    }

    _read (cb) {
        if (this.items.length) {
            const self = this
            this.drive.readFile(this.items.pop(), function(err, value) {
                if (err) return cb(err)
                self.push(value)
                cb(null)
            })
        } else {
            this.push(null)
            return cb(null)
        }
    }
}

////

function Put(db, key, value, opts, cb) {
    let { batch } = opts

    this._name = '' + key
    this._value = '' + key + '#' + value
    this._db = db
    this._release = null

    this._callback = cb
    this._batch = batch

    if (this._batch) this._start()
    else this._lock()
}

Put.prototype._start = function() {
    const self = this
    this._db.ready(function(err){
        self._finalize(err)
    })
}

Put.prototype._lock = function() {
    const self = this
    this._db._lock(function (release) {
        self._release = release
        self._finalize()
    })
}

Put.prototype._finalize = function(err) {
    const self = this

    if (err) return done(err)

    if (this._batch) {
        this._batch.append({k: this._name, v: this._value})
        return done(null)
    }

    this._db._drive.writeFile(self._name, self._value, done)

    function done(err) {
        if (self._release) self._release(self._callback, err)
        else self._callback(err)
    }
}

///////////////////

function HyperDriveDB(storage, key, opts) {
    if (!(this instanceof HyperDriveDB)) return new HyperDriveDB(storage, key, opts)

    const feedOpts = Object.assign({}, opts, { valueEncoding: 'binary', alwaysReconnect: false })
    this._drive = feedOpts.feed || hyperdrive(storage, key, feedOpts)
    this._drive.maxRequests = feedOpts.maxRequests || 256 // set max requests higher since the payload is small

    this._lock = mutexify()
}

HyperDriveDB.prototype.ready = function(cb) {
    this._drive.ready(cb)
}

HyperDriveDB.prototype.put = function(key, value, opts, cb) {
    if (typeof opts === 'function') return this.put(key, value, null, opts)
    opts = Object.assign({}, opts, {
        batch: null,
        del: 0
    })
    return new Put(this, key, value, opts, cb || noop)
}

HyperDriveDB.prototype.createReadStream = function (prefix, opts) {
    return new HDriveReadStream(this._drive, prefix)
}

HyperDriveDB.prototype.get = function (prefix, opts, cb) {
    return this._drive.readdir(prefix, opts, cb)
}