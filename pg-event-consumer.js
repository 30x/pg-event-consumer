'use strict';
const lib = require('http-helper-functions');
const db = require('./pg-event-consumer-pg.js')

var SPEEDUP = process.env.SPEEDUP || 1;
var ONEMINUTE = 60*1000/SPEEDUP;
var TWOMINUTES = 2*60*1000/SPEEDUP;
var TENMINUTES = 10*60*1000/SPEEDUP;
var ONEHOUR = 60*60*1000/SPEEDUP;

function eventConsumer(ipaddress, clientCallback) {
  this.ipaddress=ipaddress;
  this.clientCallback = clientCallback;
}

eventConsumer.prototype.processEvent = function(event) {
  this.processedEvents.setEventMark(event.index);
  this.clientCallback(event); 
}

eventConsumer.prototype.processStoredEvents = function(events) {
  for (var i=0; i< events.length; i++) {
    var event = events[i]
    console.log('processStoredEvent:', 'event:', event.index)
    var index = parseInt(event.index)
    var previouslySeen = this.processedEvents.readEventMark(index)
    this.processedEvents.setEventMark(index)
    if (!previouslySeen) {
      this.clientCallback(event)
    }  
  }
  this.processedEvents.disposeOldEvents()
}

eventConsumer.prototype.fetchStoredEvents = function(self) {
  self.processedEvents.disposeOldEvents()
  db.withEventsAfter(self.processedEvents.lastEventIndex, function(events){self.processStoredEvents(events)})
}

eventConsumer.prototype.init = function(callback) {
  var self = this
  db.withLastEventID(function(err, id) {
    if (err)
      console.log('unable to get last value of event ID')
    else {
      self.processedEvents = new BitArray(id, 1000);
      db.registerConsumer(self)
      setInterval(db.registerConsumer, ONEMINUTE, self)
      setInterval(self.fetchStoredEvents, TWOMINUTES, self)
      callback()
    }
  })
}

function BitArray(initialIndex, size) {
  console.log(`initialIndex ${initialIndex}`)
  this.processedEvents = new Uint16Array(size || 1000)  
  this.lastEventIndex = initialIndex      // database index of last processed event. This is the database index of the (firstEventOffset - 1) entry in processedEvents
  this.highestEventIndex = initialIndex   // highest database index of event processed.
  this.firstEventOffset = 0               // (offset + 1) in processedEvents of lastEventIndex. Index at which the next event will land    
}

BitArray.prototype.disposeOldEvents = function() {
  var index = this.lastEventIndex + 1
  var handled = 0
  while (this.readEventMark(index+handled)) {handled++}
  console.log(`disposing of ${handled} events. highestEventIndex: ${this.highestEventIndex} lastEventIndex: ${this.lastEventIndex} firstEventOffset: ${this.firstEventOffset}`)
  var newFirstEventOffset = this.firstEventOffset + handled
  if ((newFirstEventOffset + 1) / 16 > 1) { // shift entries left
    var firstEntry = Math.floor(newFirstEventOffset / 16)
    var lastEntry = this.entryIndex(this.highestEventIndex)
    var numberOfEntries = lastEntry - firstEntry + 1
    console.log(`copying left: firstEntry ${firstEntry} lastEntry: ${lastEntry} numberOfEntries: ${numberOfEntries}`)
    this.processedEvents.copyWithin(0, firstEntry, lastEntry+1)
    for (var i = numberOfEntries; i <= lastEntry; i++) {
      this.processedEvents[i] = 0
    }
    this.firstEventOffset = newFirstEventOffset % 16
  } else {
    this.firstEventOffset = newFirstEventOffset
  }
  this.lastEventIndex += handled
}

BitArray.prototype.bitIndex = function(index) {
  return (index - this.lastEventIndex - 1 + this.firstEventOffset) % 16;
}

BitArray.prototype.entryIndex = function(index) {
  return Math.floor((index - this.lastEventIndex - 1 + this.firstEventOffset) / 16)
}

BitArray.prototype.readEventMark = function(index) {
  var bitInx = this.bitIndex(index)
  var entryInx = this.entryIndex(index)
  var entry = this.processedEvents[entryInx]
  return (entry >> bitInx) & 1 ? true : false
}

BitArray.prototype.setEventMark = function(index) {
  var bitInx = this.bitIndex(index);
  var entryInx = this.entryIndex(index);
  var entry = this.processedEvents[entryInx];
  entry = entry | (1 << bitInx);
  this.processedEvents[entryInx] = entry;  
  this.highestEventIndex = Math.max(this.highestEventIndex, index);
}

exports.eventConsumer = eventConsumer;