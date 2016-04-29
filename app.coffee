fs = require 'fs'
jsYaml = require 'js-yaml'
R = require 'ramda'
Q = require 'q'
sync = require 'synchronize'
{ ObjectId } = require 'mongodb'
chalk = require 'chalk'

settings = jsYaml.safeLoad fs.readFileSync 'settings.yml'
{ esConnect, mongoConnect } = require('./connect') settings

jobs = [
  'profiles'
]

Q.all [ esConnect(), mongoConnect() ]
  .then ([ esClient, mongoClient ]) ->
    sync.fiber ->
      console.log chalk.green 'Connected'
      jobs.forEach (jobName) ->
        console.log jobName
        jobClass = require("./jobs/#{jobName}")
        job = new jobClass({ mongoClient, esClient })
        job.run()

  .catch (e) ->
    console.log chalk.red 'Error:', e, e.stack
    process.exit()
