'use strict'
var Pool = require('pg').Pool

var config = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE
}

var pool = new Pool(config)

function registerConsumer(ec) {
  var time = Date.now()
  var query = 'INSERT INTO consumers (ipaddress, registrationtime) values ($1, $2) ON CONFLICT (ipaddress) DO UPDATE SET registrationtime = EXCLUDED.registrationtime'
  pool.query(query, [ec.ipaddress, time], function (err, pgResult) {
    if (err) 
      console.log(`unable to register ipaddress ${ec.ipaddress} ${err}`)
    else
      console.log(`registered consumer ${ec.ipaddress} at time ${time}`)
  })
}

function withEventsAfter(index, callback) {
  var query = 'SELECT * FROM events WHERE index > $1 ORDER BY index'
  pool.query(query, [index], function(err, pgResult) {
    if (err)
      console.log(`unable to retrieve events subsequent to ${index} ${err}`)      
    else {
      console.log(`retrieved events subsequent to ${index}`)      
      callback(pgResult.rows)
    }
  })
}

function withLastEventID(callback) {
  var query = 'SELECT last_value FROM events_index_seq'
  pool.query(query, function(err, pgResult) {
    if(err) {
      console.log('error retrieving last event ID', err)
      callback(err)
    } else {
      console.log('retrieved last event ID', pgResult.rows[0].last_value)
      callback(null, parseInt(pgResult.rows[0].last_value))
    }
  })
}

(function init() {
  var query = 'CREATE TABLE IF NOT EXISTS events (index bigserial, topic text, eventtime bigint, data jsonb)'
  pool.query(query, function(err, pgResult) {
    if(err) 
      console.error('error creating events table', err)
    else {
      query = 'CREATE TABLE IF NOT EXISTS consumers (ipaddress text primary key, registrationtime bigint)'
      pool.query(query, function(err, pgResult) {
      if(err) 
        console.error('error creating consumers table', err)
      else
        console.log('event and consumers tables created or already present', err)
      })
    }
  })   
})()

exports.registerConsumer = registerConsumer
exports.withEventsAfter = withEventsAfter
exports.withLastEventID = withLastEventID