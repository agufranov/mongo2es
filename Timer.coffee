class Timer
  constructor: (@msg) ->
    @startTime = Date.now

  stop: =>
    @dt = Date.now() - @startTime

module.exports = Timer
