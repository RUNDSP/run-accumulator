/* jshint node: true */
/*global require*/
"use strict";
var aws = require('aws-sdk');
var crypto = require('crypto');
var async = require('async');
var pjson = require('./package.json');
var deagg = require('aws-kpl-deagg');

const OK = 'OK';
const ERROR = 'ERROR';
const KINESIS_SERVICE_NAME = "aws:kinesis";
const targetEncoding = "utf8";
// should KPL checksums be calculated?
const computeChecksums = true;

// Location that batched records should be written to:
const targetBucket = getEnvironmentVariable("CORE_TARGET_BUCKET", "dti-kinesis-stage/analytics/beta/");
// Filename base.  Timestamp and hash of file contents will be appended to this to ensure a unique filename.
const targetKeyBase = getEnvironmentVariable("CORE_TARGET_KEY_BASE", "analytics-kinesis");
// Record field to pull timestamp information out of.
const timestampField = getEnvironmentVariable("CORE_TIMESTAMP_FIELD", "tstamp");
// Trigger files.
const enableTriggerFiles = getEnvironmentVariable("CORE_ENABLE_TRIGGER_FILES", "false").toLowerCase() === "true";
const triggerFileName = getEnvironmentVariable("CORE_TRIGGER_FILENAME", "complete.trigger");

const setRegion = getEnvironmentVariable("AWS_REGION", "us-east-1");
const debug = getEnvironmentVariable("DEBUG", "true").toLowerCase() === "true";

var online = false;

var kinesis;
exports.kinesis = kinesis;

var s3 = new aws.S3();

function init() {
    if (!online) {
        if (!setRegion || setRegion === null || setRegion === "") {
            setRegion = "us-east-1";
            log("Warning: Setting default region " + setRegion);
        }

        log("AWS Kinesis to s3 Lambda v" + pjson.version + " in " + setRegion);

        aws.config.update({
            region: setRegion
        });

        // configure a new connection to kinesis streams, if one has not been provided
        if (!exports.kinesis) {
            log("Connecting to Amazon Kinesis Streams in " + setRegion);
            exports.kinesis = new aws.Kinesis({
                apiVersion: '2013-12-02',
                region: setRegion
            });
        }

        online = true;
    }
};

function decodeRecord(record) {
    let recordString = decodeRecordString(record);
    return JSON.parse(recordString);
}

function decodeRecordString(record) {
    return new Buffer(record.data, 'base64').toString(targetEncoding);
}

function log(message) {
    if (debug) {
        console.log(message);
    }
}

function getEnvironmentVariable(varname, defaultvalue)
{
    var result = process.env[varname];
    if(result!=undefined)
        return result;
    else
        return defaultvalue;
}

var addNewlineTransformer = function (data) {
    // emitting a new buffer as text with newline
    return new Buffer(data + "\n", targetEncoding);
};

/** Convert JSON data to its String representation */
var jsonToStringTransformer = function (data) {
    // emitting a new buffer as text with newline
    return new Buffer(JSON.stringify(data) + "\n", targetEncoding);
};

/** literally nothing at all transformer - just wrap the object in a buffer */
var doNothingTransformer = function (data) {
    // emitting a new buffer as text with newline
    return new Buffer(data);
};

/*
 * create the transformer instance.
 */
var transformer = jsonToStringTransformer.bind(undefined);


/** function to extract the kinesis stream name from a kinesis stream ARN */
function getStreamName(arn) {
    log('arn: ' + arn);
    try {
        var eventSourceARNTokens = arn.split(":");
        return eventSourceARNTokens[5].split("/")[1];
    } catch (e) {
        log("Malformed Kinesis Stream ARN");
        return;
    }
};

function dateToBucketURL(date) {
    var bucket = targetBucket + getDatePartStringFolder(date);
    return bucket;
};

//create unique hash for each file
function checksum(str, algorithm, encoding) {
    return crypto
        .createHash(algorithm || 'md5')
        .update(str, 'utf8')
        .digest(encoding || 'hex');
};

function leftPadZero(number, targetLength) {
    var output = number + '';
    while (output.length < targetLength) {
        output = '0' + output;
    }
    return output;
}

function getDatePartStringFolder(date) {
    let year = date.getUTCFullYear();
    let month = leftPadZero(date.getUTCMonth() + 1, 2);
    let day = leftPadZero(date.getUTCDate(), 2);
    let hour = leftPadZero(date.getUTCHours(), 2);

    let datePath = year + '/' + month + '/' + day + '/' + hour;
    return datePath;
}

function getDatePartStringFile(date) {
    let year = date.getUTCFullYear();
    let month = leftPadZero(date.getUTCMonth() + 1, 2);
    let day = leftPadZero(date.getUTCDate(), 2);
    let hour = leftPadZero(date.getUTCHours(), 2);
    let minute = leftPadZero(date.getUTCMinutes(), 2);
    let second = leftPadZero(date.getUTCSeconds(), 2);
    
    let datePath = year + '-' + month + '-' + day + '-' + hour + '-' + minute + '-' + second;
    return datePath;
}

function uploadFileStreamToBucket(timestamp, data, callback) {
    var stringifiedData = JSON.stringify(data);
    var hashHex = checksum(stringifiedData, 'md5');

    var date = new Date(timestamp * 1000);

    var bucket = dateToBucketURL(date);
    var key = targetKeyBase + '-' + getDatePartStringFile(date) + '-' + hashHex

    log('key: ' + key);

    var params = {
        Bucket: bucket,
        Key: key,
        Body: data
    };

    s3.upload(params, function (err, data) {
        log('s3.upload response:');
        if (err != null) {
            log(err);
            callback(err, null);
        } else {
            log(data);
            callback(null, data);
        }
    });
};

/** AWS Lambda event handler */
exports.handler = function (event, context) {
    let finish = function (err, event, status, message) {
        log("Processing Complete!!!");

        // log the event if we've failed
        if (status !== OK) {
            if (message) {
                log(message);
            }

            // ensure that Lambda doesn't checkpoint to kinesis on error
            context.done(status, JSON.stringify(message));
        } else {
            context.done(null, message);
        }
    };

    let processBatchRecords = function (records, start, end, callback) {
        log('Processing batch of records');
        log('start: ' + JSON.stringify(start));
        log('end: ' + JSON.stringify(end));
        // get the set of batch offsets based on the transformed record sizes
        let recordsInBatch = records.slice(start, end + 1);
        log('recordsInBatch.length: ' + recordsInBatch.length);
        let transformedRecordsArray = recordsInBatch.map(r => transformer(r));
        let transformedRecords = transformedRecordsArray.join('');

        // Grab the timestamp from the first record to use for S3 bucket/key calculations.
        var timestamp = records[start][timestampField];

        writeToS3(timestamp, transformedRecords, callback);
    };

    /**
     * function which forwards a batch of kinesis records to a s3
     */
    let writeToS3 = function (timestamp, data, callback) {
        log('Writing to s3');

        var startTime = new Date().getTime();
        uploadFileStreamToBucket(timestamp, data, function(err, result) {
            if (err != null) {
                log(err);
                callback(err, null);
            }
            else {
                var elapsedMs = new Date().getTime() - startTime;
                log("Successfully wrote records to s3 in " + elapsedMs + " ms");
                callback(null, result);
            }
        });
    };

    /** Creates a trigger file if one does not already exist. **/
    let writeTriggerFile = function (date, callback) {
        var bucket = dateToBucketURL(date);

        var params = {
            Bucket: bucket,
            Key: triggerFileName,
            Body: "DONE"
        };

        //check if intended bucket exists, if not create it
        s3.headObject(params, function (err, data) {
            if (err) {
                //if bucket does not exist then create it so we can upload file
                s3.upload(params, function (err, data) {
                    callback(err, data.Bucket + "/" + data.Key);
                });
            }
            else {
                callback(null, bucket + "/" + triggerFileName);
            }
        });
    };

    let processEvent = function (event, serviceName, streamName, callback) {
        log("Batching and writing " + event.Records.length + " " + serviceName + " records to S3 from stream " + streamName + ".");

        let records = event["Records"];

        if (records.length > 0) {
            var batchStartIndex = 0;
            let deaggRecords = [];

            async.map(records, function(item, recordCallback) {
                // run the record through the KPL deaggregator
                deagg.deaggregateSync(item.kinesis, computeChecksums, function(err, userRecords) {
                    if (err) {
                        recordCallback(err);
                    } else {
                        recordCallback(null, userRecords);
                    }
                });
            },
            function (err, extractedUserRecords) {
                if (err) {
                    finish(err, ERROR);
                } else {
                    // extractedUserRecords will be array[array[Object]], so
                    // flatten to array[Object]
                    var userRecords = [].concat.apply([], extractedUserRecords);
                    
                    var userRecordsDecoded = userRecords.map(ur => decodeRecord(ur));

                    var firstRecordDate = new Date(userRecordsDecoded[0][timestampField] * 1000);
                    var batchStartHour = firstRecordDate.getUTCHours()

                    var batches = [];

                    // Find an hour boundary.
                    for (var i = 1; i < userRecordsDecoded.length; i++) {
                        var eventDate = new Date(userRecordsDecoded[i][timestampField] * 1000);
                        if (eventDate.getUTCHours() != batchStartHour) {
                            // Hour boundary...record the batch indices.
                            batches.push({
                                start: batchStartIndex,
                                end: i - 1
                            });
                            batchStartHour = eventDate.getUTCHours();
                            batchStartIndex = i;
                        }
                    }
                    // Make sure we get the end of the range.
                    batches.push({
                        start: batchStartIndex,
                        end: userRecordsDecoded.length - 1
                    });

                    // Run processBatchRecords for each batch identified.
                    log('Found ' + batches.length + ' batches in ' + userRecordsDecoded.length + " records.");
                    async.each(batches, function(batch, callback) {
                        let batchIndex = batches.indexOf(batch);
                        let batchCount = batches.length;
                        let batchNumber = batchIndex + 1;
                        log("Processing batch " + batchNumber + " of " + batchCount + ".");
                        
                        processBatchRecords(
                            userRecordsDecoded,
                            batch.start,
                            batch.end,
                            function (err, result) {
                                if (err != null) {
                                    log("Error processing batch " + batchNumber);
                                    callback(err);
                                }
                                else {
                                    log("Successfully processed batch " + batchNumber + " of " + batchCount + ".");
                                    log(result);
                                    callback(null);
                                }
                            }
                        );
                    },
                    function(err) {
                        if (err != null) {
                            log('One or more batches failed to save.');
                            log(err);
                            callback(err, null);
                        } else {
                            log(batches.length + " batches have been saved.");

                            if (enableTriggerFiles) {
                                // Create trigger file for previous hour if we only have one batch (no hour boundaries detected).
                                if (batches.length == 1) {
                                    let ts = new Date(firstRecordDate);
                                    ts.setHours(ts.getHours() - 1);
                                    writeTriggerFile(ts, function(err, result) {
                                        if (err != null) {
                                            log("Error writing trigger file.");
                                            log(err);
                                            callback(err, null);
                                        }
                                        else {
                                            log("Trigger file written: " + result);
                                            callback(null, records.length);
                                        }
                                    })
                                } else {
                                    // Multiple batches...don't write a trigger file.
                                    callback(null, records.length);
                                }
                            }
                            else {
                                // No trigger files necessary.
                                callback(null, records.length);
                            }
                        }
                    });
                }
            });
        }
        else {
            // No records to process.
            callback(null, 0);
        }
    };

    /** End Runtime Functions */

    log(JSON.stringify(event));

    // fail the function if the wrong event source type is being sent, or if
    // there is no data, etc
    let noProcessStatus = ERROR;
    let noProcessReason;
    let serviceName;
    let streamName;

    if (!event.Records || event.Records.length === 0) {
        noProcessReason = "Event contains no Data";
        // not fatal - just got an empty event
        noProcessStatus = OK;
    } else {
        // there are records in this event
        let firstRecord = event.Records[0];
        if (firstRecord.eventSource === KINESIS_SERVICE_NAME) {
            serviceName = firstRecord.eventSource;
            streamName = getStreamName(firstRecord.eventSourceARN);
        } else {
            noProcessReason = "Invalid Event Source " + firstRecord.eventSource;
        }

        // currently hard coded around the 1.0 kinesis event schema
        if (firstRecord.kinesis && firstRecord.kinesis.kinesisSchemaVersion !== "1.0") {
            noProcessReason = "Unsupported Kinesis Event Schema Version " + firstRecord.kinesis.kinesisSchemaVersion;
        }
    }

    if (noProcessReason) {
        // terminate if there were any non process reasons
        finish(null, event, noProcessStatus, noProcessReason);
    } else {
        init();

        // delivery stream is cached
        processEvent(event, serviceName, streamName, function (err, result) {
            if (err != null) {
                finish(err, event, ERROR, "Error processing event.");
            }
            else {
                finish(null, event, OK, result);
            }
        });
    }
};
