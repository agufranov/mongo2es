{ EventEmitter } = require 'events'
R = require 'ramda'
Q = require 'q'

class ChunkMigrator extends EventEmitter
  constructor: ({
    @getNextChunkFn
    @insertFn
    @isExhaustedFn
    @transformFn = R.identity
  }) ->

  _nextStep: (chunk) =>
    actions = R.reject R.isNil, [
      @getNextChunkFn()
      @insertFn chunk.map @transformFn if chunk?.length > 0
    ]
    Q.all actions
      .then ([ nextChunk ]) =>
        if @isExhaustedFn()
          @emit 'done'
        else
          @emit 'progress'
          @_nextStep nextChunk
      .catch (err) =>
        @emit 'error', err

  migrate: => @_nextStep()

module.exports = ChunkMigrator
