const kappa = require('kappa-record-db')
const leveldb = require('level')

module.exports = KappaDB

function KappaDB(storage, key) {
    if (!(this instanceof KappaDB)) return new KappaDB(storage, key)

    const opts = {
        name: 'db1',
        alias: 'w1',
        validate: false
    }

    if (key) opts.key = key
    if (typeof storage === 'string') {
        opts.storage = storage
        opts.db = leveldb(storage)
    }

    this._kappa = new kappa(opts)
}

KappaDB.prototype.ready = function(cb) {
    this._kappa.ready(cb)
}

KappaDB.prototype.batch = function(opts, cb) {
    return this._kappa.batch(opts, cb)
}

KappaDB.prototype.replicate = function(opts) {
    return this._kappa.replicate(opts.initiator, opts)
}

KappaDB.prototype.put = function(key, value, cb) {
    return this._kappa.put({schema: 'doc', value: value, id: key}, cb)
}

KappaDB.prototype.once = function(name, cb) {
    return this._kappa.once(name, cb)
}

KappaDB.prototype.get = function(key, opts, cb) {
    return this._kappa.query('records', {schema: 'doc', id: key}, (err, records) => {
        cb(err)
    })
}
