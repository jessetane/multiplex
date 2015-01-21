var protobuf = require('protocol-buffers')
var duplexify = require('duplexify')
var lpstream = require('length-prefixed-stream')
var through = require('through2')
var fs = require('fs')

var messages = protobuf(fs.readFileSync(__dirname+'/schema.proto'))
var TYPE = messages.TYPE

var multiplex = function(opts, onstream) {
  if (typeof opts === 'function') return multiplex(null, opts)
  if (!opts) opts = {}

  var channels = 0
  var streams = {}
  var encode = lpstream.encode()
  var decode = lpstream.decode()
  var dup = duplexify.obj(decode, encode)

  var addChannel = function(channel, name) {
    var stream = duplexify.obj()
    var readable = through.obj()
    var writable = through.obj(function(data, enc, cb) {
      if (typeof data === 'string') data = new Buffer(data)
      encode.write(messages.Frame.encode({channel:-channel, data:data, name:name}), cb)
      name = null
    })

    var destroy = function(type, data) {
      if (streams[channel] !== readable) return
      encode.write(messages.Frame.encode({channel:-channel, type:type, data:data, name:name}))
      streams[channel] = null
    }

    streams[channel] = readable

    stream.meta = name
    stream.setReadable(readable)
    stream.setWritable(writable)

    var onerror = function(err) {
      destroy(TYPE.ERROR, err && new Buffer(err.message))
    }

    readable.once('destroy', function(err) {
      stream.removeListener('close', onerror)
      stream.removeListener('error', onerror)
      stream.destroy(err)
    })

    stream.on('close', onerror)
    if (opts.error) stream.on('error', onerror)

    writable.on('finish', function() {
      destroy(TYPE.END)
      readable.end()
    })

    return stream
  }

  var decodeFrame = function(data) {
    try {
      return messages.Frame.decode(data)
    } catch (err) {
      return null
    }
  }

  var decoder = function(data, enc, cb) {
    var frame = decodeFrame(data)
    if (!frame) return dup.destroy(new Error('Invalid data'))

    var channel = frame.channel
    var reply = channel > 0

    if (!reply && streams[channel] === undefined && onstream) {
      onstream(addChannel(channel, frame.name), frame.name || channel)
    }

    var stream = streams[channel]

    if (!stream) return cb()

    switch (frame.type) {
      case TYPE.DATA:
      return stream.write(frame.data, cb)

      case TYPE.END:
      delete streams[channel]
      stream.end()
      return cb()

      case TYPE.ERROR:
      delete streams[channel]
      stream.emit('destroy', frame.data && new Error(frame.data.toString()))
      return cb()
    }

    cb()
  }

  decode.pipe(through.obj(decoder))

  dup.on('close', function() {
    Object.keys(streams).forEach(function(key) {
      var readable = streams[key]
      if (readable) readable.emit('destroy')
    })
  })

  dup.on('finish', function() {
    Object.keys(streams).forEach(function(key) {
      var readable = streams[key]
      if (readable) readable.end()
    })
    encode.end()
  })

  dup.createStream = function(name) {
    return addChannel(++channels, name && ''+name)
  }

  return dup
}

module.exports = multiplex