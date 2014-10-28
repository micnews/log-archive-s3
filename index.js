var s3 = require('s3')

var path = require('path')

var pull    = require('pull-stream')
var glob    = require('pull-glob')
var paramap = require('pull-paramap')
var fs      = require('fs')

function ls (pattern, cb) {

  var _file
  pull(
    glob(pattern),
    pull.asyncMap(function (file, cb) {
      fs.stat(file, function (err, stat) {
        if(err) return cb()
        stat.filename = file
        cb(null, stat)
      })
    }),
    pull.collect(cb)
  )
}

function size (ary) {
  return ary.reduce(function (sum, stat) {
    return sum += stat.size
  }, 0) || 0
}

//scan the directory, and if it's larger than a predefined size,
//upload the oldest files to s3 until we are under the limit & repeat.

module.exports = function (config) {

  if(!config)
    throw new Error('log-rotate-s3: *must* have config object')
  if(!config.logDir)
    throw new Error('log-rotate-s3: *must* have config.logDir')
  if(!config.bucket)
    throw new Error('log-rotate-s3: *must* have config.bucket')

  var max = config.maxUnarchived || 500*1024*1024
  var pattern = config.glob || '*/*/*/*'
  var interval = config.interval || 10*60*1000 // ten minutes
  var client = s3.createClient({
    s3Options: config
  })

  ;(function scan () {
    ls(path.join(config.logDir, pattern), function (err, ls) {
      var total = size(ls)
      console.log(total, max, total - max)

      ;(function archive () {
        total = size(ls)
        if(total < max)
          return setTimeout(scan, interval)

        var file = ls.shift()
        var basename = path.basename(file.filename.replace(/\.log$/, ''))

        console.log('space:', total, '>', max, 'archiving:' + basename)

        var uploader =
          client.uploadFile({
            localFile: file.filename,
            s3Params: {
              Bucket: config.bucket || 'megafunnel-test',
              Key: basename
            }
          })
          .on('error', function (err) {
            //the s3 module does weird stuff with my errors.
            //work around...
            var _err = new Error(err.message)
            _err.stack = err.stack + _err.stack
            throw _err
          })
          .on('end', function () {
            fs.unlink(file.filename, archive)
          })

      })()
    })

  })()

}

