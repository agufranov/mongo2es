sync = require 'synchronize'
R = require 'ramda'
Q = require 'q'

class ElasticBulkWriter
  constructor: ({
    elasticsearchClient: @_client
    index: @_index
    type: @_type
  }) ->
    sync @_client, 'bulk', 'count'
    sync @_client.indices, 'exists', 'create', 'delete', 'putMapping', 'putSettings', 'close', 'open'

  recreateIndex: =>
    if @_client.indices.exists(index: @_index)
      @_client.indices.delete index: @_index
    @_client.indices.create index: @_index

  _toBulk: (chunk) =>
    R.chain((doc) => [
      { index: { _index: @_index, _type: @_type, _id: doc._id } }
      R.omit '_id', doc
    ])(chunk)

  _getCount: =>
    @_client.count(index: @_index, type: @_type).count

  bulkWrite: (chunk) =>
    Q.Promise (resolve, reject) =>
      countBefore = @_getCount()
      chunkCount = chunk.length
      neededCount = countBefore + chunkCount
      @_client.bulk { body: @_toBulk chunk }
      i = 0
      interval = setInterval =>
        sync.fiber =>
          currentCount = @_getCount()
          if currentCount is neededCount
            clearInterval interval
            resolve()
          else
            if i > 10
              console.log "Max retires exceeded, exiting"
              clearInterval interval
              reject "Max retires exceeded, exiting"
            i++
            console.log "count: #{currentCount}, needed: #{neededCount}, retries: #{i}"
      , 2000


  putMapping: (mapping) =>
    @_client.indices.putMapping
      index: @_index
      type: @_type
      body:
        "#{@_type}": mapping

  putSettings: (settings) =>
    @_client.indices.close index: @_index
    @_client.indices.putSettings
      index: @_index
      body: settings
    @_client.indices.open index: @_index

module.exports = ElasticBulkWriter
