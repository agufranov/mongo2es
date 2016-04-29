sync = require 'synchronize'
R = require 'ramda'

class ElasticBulkWriter
  constructor: ({
    elasticsearchClient: @_client
    index: @_index
    type: @_type
  }) ->
    sync @_client, 'bulk'
    sync @_client.indices, 'exists', 'create', 'delete', 'putMapping'

  recreateIndex: =>
    if @_client.indices.exists(index: @_index)
      @_client.indices.delete index: @_index
    @_client.indices.create index: @_index

  _toBulk: (chunk) =>
    R.chain((doc) => [
      { index: { _index: @_index, _type: @_type, _id: doc._id } }
      R.omit '_id', doc
    ])(chunk)

  bulkWrite: (chunk) =>
    @_client.bulk body: @_toBulk chunk

  putMapping: (mapping) =>
    @_client.indices.putMapping
      index: @_index
      type: @_type
      body:
        "#{@_type}": mapping

module.exports = ElasticBulkWriter
