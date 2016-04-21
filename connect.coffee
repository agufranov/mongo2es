{ MongoClient } = require 'mongodb'
{ Client: EsClient } = require 'elasticsearch'
Q = require 'q'
chalk = require 'chalk'

module.exports = (settings) ->
  throw new Error 'No settings provided in settings.yml' unless settings?

  esConnect: ->
    Q.Promise (resolve, reject) ->
      url = settings?.esUrl
      return reject 'No Elasticsearch url provided in settings (settings.esUrl)' unless url
      console.log chalk.yellow "Connecting to Elasticsearch at #{url}..."
      esClient = new EsClient host: url
      esClient.ping (err, result) ->
          return resolve esClient unless err?
          console.log "Error connecting to Elasticsearch at #{url}"
          reject err

  mongoConnect: ->
    Q.Promise (resolve, reject) ->
      url = settings?.mongoUrl
      return reject 'No MongoDB url provided in settings (settings.mongoUrl)' unless url
      console.log chalk.yellow "Connecting to MongoDB at #{url}..."
      MongoClient.connect settings.mongoUrl, (err, mongoClient) ->
        return resolve mongoClient unless err?
        console.log "Error connecting to MongoDB at #{url}"
        reject err
