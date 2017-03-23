"use strict";

var async = require('async');
var deagg = require('aws-kpl-deagg');
var mongo = require('mongodb');
const computeChecksums = true;

var MongoClient = mongo.MongoClient;

var db

function init(){
// initialize mongo 
MongoClient.connect('', (err, database) => {
  if (err) return console.log(err)
  db = database
})
    
}

function updateCountMongo(payload){
    let winrecord = JSON.parse(payload);
    console.log("Aggregating by plid: " + winrecord["plid"] + " and exchange: " + winrecord["exchange"]);
}

exports.handler = function(event, context, callback) {
    event.Records.forEach(function(record) {
        // Kinesis data is base64 encoded so decode here
        var payload = new Buffer(record.kinesis.data, 'base64').toString('ascii');
        console.log('Decoded payload:', payload);
        updateCountMongo(payload);
    });
    callback(null, "message");
};

