const inherits = require('util').inherits
const RandomAccess = require('random-access-storage')

module.exports = RandomAccessWithStat

function RandomAccessWithStat(stat, storageCtor) {
  if (!(this instanceof RandomAccessWithStat)) return new RandomAccessWithStat(stat, storageCtor)

  RandomAccess.call(this)

  this._storage = storageCtor()
  this.readStat = new Map()
  this.writeStat = new Map()

  stat.readStat = this.readStat
  stat.writeStat = this.writeStat
}

inherits(RandomAccessWithStat, RandomAccess)

RandomAccessWithStat.prototype._open = function (req) {
  return this._storage._open(req)
}

RandomAccessWithStat.prototype._openReadonly = function (req) {
  return this._storage._openReadonly(req)
}

RandomAccessWithStat.prototype._write = function (req) {
  let v = this.writeStat.get(req.size)
  this.writeStat.set(req.size, v ? v + 1 : 1)
  return this._storage._write(req)
}

RandomAccessWithStat.prototype._read = function (req) {
  let v = this.readStat.get(req.size)
  this.readStat.set(req.size, v ? v + 1 : 1)
  return this._storage._read(req)
}

RandomAccessWithStat.prototype._del = function (req) {
  return this._storage._del(req)
}

RandomAccessWithStat.prototype._stat = function (req) {
  return this._storage._stat(req)
}

RandomAccessWithStat.prototype._close = function (req) {
  return this._storage._close(req)
}

RandomAccessWithStat.prototype._destroy = function (req) {
  return this._storage._destroy(req)
}
