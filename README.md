This repo is for the rewrite to move away from Kinesis Firehose to a system where the AWS Lambda batches and then moves events to S3.
Current program flow is to receive events and for each event write it to a file in the correct UTC timestamp bucket following the dti-kinesis/v1/YYYY/MM/DD/hh
convention.
After a batch of entirely new hour's data is seen, e.g. only events that happen after 10, then a trigger file is written to the previous hour's bucket to trigger
a spark job

To upload to AWS Lambda dashboard run npm install to install all dependencies listed in package.json, zip the index.js, package.json and the node_modules
folder into a zip file and upload the zip.# run-accumulator
