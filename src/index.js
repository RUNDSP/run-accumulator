"use strict";
var async = require('async');
var mongoose = require('mongoose');

var Schema = mongoose.Schema;

// create a schema
var winsSchema = new Schema({
    plid : String,
    exchange : String,
    insert_date : String,
    insert_hour : Number,
    count : Number
});

//var uri = 'mongodb://127.0.0.1:27017/run_test'
var uri = 'mongodb://prod-mongo-secondary-rs0.rundsp.com:27017/test'

mongoose.Promise = global.Promise;
var db = mongoose.connect(uri);
console.log("mongoose : " + mongoose.connection.readyState);
var Wins = db.model('Wins', winsSchema, 'wins_count');


exports.handler = function(event, context,callback) {


    let getKey = function(record){
        return {'plid': record['plid'], 'exchange' : record['exchange'],'insert_date' : record['insert_date'],
                    'insert_hour': record['insert_hour']};
    }

    let insertDocument = function(record,callback) {
        console.log("inside insertDocument");
        record.save( function(err , record ){
            if(err){
                console.log("Failed insert");
                callback(err, "Error while updating record " + record);
            }else{
                console.log("Success insert");
                callback(null, "Successfully updated record");    
            }
        });
    };

    let updateDocument = function(record,result,callback) {
        console.log("Inside updateDocument");
        Wins.updateOne(
          getKey(record),
          {
            $set: { "count": parseInt(result[0]['count']) + parseInt(record['count'])}
          }, function(err, results) {
            if(err){
                console.log("Failed update");
                callback(err, "Error while updating record " + record);
            }else{
                 console.log("Success update")
                 callback(null, "Successfully updated record");    
            }
         
       });
    }



    var findKey = function(record,callback){
        console.log("Inside findkey");
        Wins.find(getKey(record) , function (err, result) {
            if (err) {
                console.log(err);
                callback(err,"Error in finding record " + record + err);
            } else if (result.length) {
                console.log('Found:', result);
                updateDocument(record,result,callback);
            } else {
                console.log('No document(s) found with defined "find" criteria!');
                insertDocument(record,callback);
            }
        });
    }

    var getRecord = function(payload){
        let winrecord = JSON.parse(payload);
        let d = new Date();
        return new Wins({
            "plid" : winrecord["plid"],
            "exchange" : winrecord["exchange"],
            "insert_date" : d.toISOString().slice(0,10),
            "insert_hour" : d.getHours(),
            "count" : 1
        });
    }

    async.each(event.Records , 
        function(record,callback) {
            // Kinesis data is base64 encoded so decode here
            var payload = new Buffer(record.kinesis.data, 'base64').toString('ascii');
            console.log('Decoded payload:', payload);
            var key = getRecord(payload);
            console.log(key);
            findKey(key,function (err, result) {
                                if (err != null) {
                                    console.log("Error processing batch");
                                    callback(err);
                                }
                                else {
                                    console.log("Successfully processed batch");
                                    console.log(result);
                                    callback(null);
                                }
                            });
        }, function(err){
            db.disconnect();
            if(err){
                console.log("Failed to process" + err);
                context.done(null, "Error");
            }else{
                console.log("All good");
                context.done(null,"Done");
            }
        }   

    );

};



// For testing 
/*
class Context {
    done(object, message){
        console.log("Context" + message);
    }

}
var event = require('../sampleTest.json');
var context = new Context();

exports.handler(event,context,function(object,message){
    console.log("Main: " + message);
    process.exit();
});
*/
