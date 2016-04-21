Q = require 'q'
R = require 'ramda'

class MongoPaginator
  constructor: (@_db, collectionName, @_pageSize, @_query) ->
    @_collection = @_db.collection collectionName

  _lastId: null

  _exhausted = false

  getExhausted: => @_exhausted

  _nextPageQuery: =>
    R.reject R.isNil, R.flatten [
      { $match: { _id: { $gt: @_lastId } } } if @_lastId?
      @_query
      { $sort: { _id: 1 } }
      { $limit: @_pageSize }
    ]

  getNextPage: =>
    Q.Promise (resolve, reject) =>
      @_collection.aggregate @_nextPageQuery()
        .toArray (err, result) =>
          return reject err if err?
          @_lastId = R.last(result)?._id
          console.log "lastId: #{@_lastId}"
          @_exhausted = true unless @_lastId?
          resolve result

module.exports = MongoPaginator
