fs = require 'fs'
jsYaml = require 'js-yaml'
R = require 'ramda'
Q = require 'q'
sync = require 'synchronize'
{ ObjectId } = require 'mongodb'
chalk = require 'chalk'
MongoPaginator = require './MongoPaginator'
ChunkMigrator = require './ChunkMigrator'

settings = jsYaml.safeLoad fs.readFileSync 'settings.yml'
{ esConnect, mongoConnect } = require('./connect') settings

sync.fiber ->
  Q.all [ esConnect(), mongoConnect() ]
    .then ([ esClient, mongoClient ]) ->
      sync esClient.indices, 'create', 'delete'
      console.log chalk.green 'Connected'
      mp = new MongoPaginator mongoClient, 'test_es_migration', 2

      bulkWrite = (result) ->
        console.log result
        Q.delay 500
          .then ->
            console.log "Bulk write #{result.length} items"

      mgr = new ChunkMigrator
        getNextChunkFn: mp.getNextPage
        insertFn: bulkWrite
        isExhaustedFn: mp.getExhausted
        # transformFn: R.prop '_id'

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
