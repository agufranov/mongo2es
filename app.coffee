fs = require 'fs'
jsYaml = require 'js-yaml'
R = require 'ramda'
Q = require 'q'
sync = require 'synchronize'
{ ObjectId } = require 'mongodb'
chalk = require 'chalk'
MongoPaginator = require './MongoPaginator'
ChunkMigrator = require './ChunkMigrator'
ElasticBulkWriter = require './ElasticBulkWriter'

settings = jsYaml.safeLoad fs.readFileSync 'settings.yml'
{ esConnect, mongoConnect } = require('./connect') settings

Q.all [ esConnect(), mongoConnect() ]
  .then ([ esClient, mongoClient ]) ->
    sync.fiber ->
      console.log chalk.green 'Connected'
      mp = new MongoPaginator mongoClient, 'debug_topic_labels', 2000

      bw = new ElasticBulkWriter esClient, 'pi_registry_topic_labels', 'all'
      bw.recreateIndex()
      bw.putMapping
        properties:
          suggest:
            type: 'completion'
            analyzer: 'simple'
            search_analyzer: 'simple'
            payloads: true
      # return
      bulkWrite = (result) ->
        bw.bulkWrite result
        # console.log result
        # Q.delay 500
        #   .then ->
        #     console.log "Bulk write #{result.length} items"

      mgr = new ChunkMigrator
        getNextChunkFn: mp.getNextPage
        insertFn: bulkWrite
        isExhaustedFn: mp.getExhausted
        transformFn: (doc) =>
          R.merge doc,
            suggest:
              input: [doc.readable_label]
              output: doc.readable_label
              payload: doc
              weight: doc.counts.people_count

      mgr.on 'progress', -> console.log 'mgr progress', arguments
      mgr.on 'error', ->
        console.log 'mgr error', arguments
        process.exit()
      mgr.on 'done', ->
        console.log 'mgr done'
        process.exit()
      mgr.migrate()
      1

    .catch (e) ->
      console.log chalk.red 'Error:', e, e.stack
      process.exit()
