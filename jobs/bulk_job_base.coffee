R = require 'ramda'
MongoPaginator = require '../MongoPaginator'
ChunkMigrator = require '../ChunkMigrator'
ElasticBulkWriter = require '../ElasticBulkWriter'

class BulkJobBase
  constructor: ({
    mongoClient
    esClient
    collectionName
    pageSize
    query
    esIndex
    esType
    mapping
  }) ->
    mongoPaginator = new MongoPaginator {
      db: mongoClient
      collectionName
      pageSize
      query
    }

    bulkWriter = new ElasticBulkWriter
      elasticsearchClient: esClient
      index: esIndex
      type: esType

    bulkWriter.recreateIndex()
    console.log "Recreated index #{esIndex}"
    bulkWriter.putMapping mapping

    @mgr = new ChunkMigrator
      getNextChunkFn: mongoPaginator.getNextPage
      insertFn: bulkWriter.bulkWrite
      isExhaustedFn: mongoPaginator.getExhausted
      transformFn: (doc) =>
        R.merge doc,
          suggest:
            input: doc.full_name.split ' '
            output: doc.full_name
            payload: doc

    @mgr.on 'progress', -> console.log 'mgr progress', arguments
    @mgr.on 'error', ->
      console.log 'mgr error', arguments
      process.exit()
    @mgr.on 'done', ->
      console.log 'mgr done'
      process.exit()

  run: =>
    console.log 'running'
    @mgr.migrate()

module.exports = BulkJobBase
