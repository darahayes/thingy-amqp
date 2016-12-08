'use strict'

const amqp = require('amqplib/callback_api')
const parse = require('fast-json-parse')
const stringify = require('fast-safe-stringify')

module.exports = function(thingy, opts) {

  opts.maxRetries = opts.maxRetries || 10
  var connection = null
  var channel = null

  function start(done) {

    var retry = 0

    var timer = setInterval(() => {
      retry++
      connect(opts, (err) => {
        if (err) {
          if (retry <= opts.maxRetries) {
            return thingy.logger.error({error: 'AMQP connect failed', retry: retry})
          }
          clearInterval(timer)
          done(err)
        }
        clearInterval(timer)
        done()
      })
    }, 1000)
  }

  function connect(opts, done) {
    amqp.connect(opts.url, (err, conn) => {
      if (err) return done(err)
      connection = conn
      conn.createChannel((err, ch) => {
        channel = ch
        channel.assertQueue(opts.q, {durable: true})
        channel.prefetch(1)
        channel.consume(opts.q, (message) => {
          parseMessage(message, (err, parsed) => {
            if (err) {
              thingy.logger.error({error: 'Invalid JSON message', message: message})
              return channel.ack(message)
            }
            thingy.received(message, (err, result) => {
              channel.ack(message)
            })
          })
        })
        done()
      })
    })
  }

  function dispatch(key, msg) {
    channel.publish('', key, Buffer.from(stringify(msg)))
  }

  function parseMessage(message, done) {
    var parsed = parse(message.content)
    if (parsed.err) {
      message.content = message.content.toString()
      return done(parsed.err)
    }
    message.content = parsed.value
    done(null, message)
  }

  function exit(error) {
    thingy.logger.error(error)
    process.exit(1)
  }

  return {
    start: start,
    dispatch: dispatch
  }
}
