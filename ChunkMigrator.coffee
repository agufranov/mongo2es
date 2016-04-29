{ EventEmitter } = require 'events'
R = require 'ramda'
Q = require 'q'
Timer = require './Timer'

class ChunkMigrator extends EventEmitter
  constructor: ({
    @getNextChunkFn
    @insertFn
    @isExhaustedFn
    @transformFn = R.identity
  }) ->

  _readsTotal: 0
  _writesTotal: 0

  _nextStep: (chunk) =>
    actions = R.reject R.isNil, [
      @getNextChunkFn()
      @insertFn chunk.map @transformFn if chunk?.length > 0
    ]

    Q.all actions

      .then ([ nextChunk ]) =>
        @_writesTotal += chunk.length if chunk?.length > 0
        @_readsTotal += nextChunk.length if nextChunk?.length > 0

        @emit 'progress', { @_writesTotal, @_readsTotal }

        if @isExhaustedFn()
          @emit 'done'
        else
          setTimeout => @_nextStep nextChunk

      .catch (err) =>
        @emit 'error', err

  migrate: => @_nextStep()

module.exports = ChunkMigrator
