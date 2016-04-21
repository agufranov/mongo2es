fs = require 'fs'
jsYaml = require 'js-yaml'
Q = require 'q'

settings = jsYaml.safeLoad fs.readFileSync 'settings.yml'
{ esConnect, mongoConnect } = require('./connect') settings

Q.all [ esConnect(), mongoConnect() ]
  .then (esClient, mongoClient) ->
    console.log 'Connected'
  .catch (e) ->
    console.log 'Error', e
    process.exit()
