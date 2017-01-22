'use strict';
var https = require('https');
var url = require('url');
var AWS = require('aws-sdk');
var sleep = require('sleep');
var EventEmitter = require("events").EventEmitter;
var responseMsg = new EventEmitter();

module.exports.sendMessage = (event, context, callback) => {
  var messageJSON;

  if(event.Records){
      messageJSON = JSON.parse(event.Records[0].Sns.Message);
  }
  else{
      messageJSON = JSON.parse(event.body);
  }

  if(!messageJSON.tries)
    messageJSON.tries = 0;

  messageJSON.tries += 1;

  if(messageJSON.tries > 5){
    var response = {
      statusCode: 400,
      body: JSON.stringify({
        "ERROR" : "Mensaje no enviado: supera numero maximo de intentos (5)"
      })
    };
    callback(null, response);
  }
  else{
    if(messageJSON.tries > 1) sleep(10000); //fixed to 10 seconds but can be replaced (replace timeout of the function too)

    var postData = JSON.stringify(messageJSON.body);

    var urlDest = url.parse(messageJSON.url);

    var options = {
      hostname: urlDest.host,
      port: urlDest.port,
      path: urlDest.pathname,
      method: messageJSON.method,
      headers: {
        'Content-Type' : 'application/json',
        'Content-Length': postData.length
      }
    };

    var req = https.request(options, (res) => {
      var data = "";
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        console.log(`DATA CHUNK: ${chunk}`);
        data += chunk;
      });
      res.on('end', () => {
        var dataJSON = JSON.parse(data);

        var response = {
          statusCode: 200,
          body: JSON.stringify({
            "Success" : dataJSON
            })
        };

        callback(null, response);
      });
    });

    req.on('error', (e) => {
      var error = `ERROR: ${e.message}`;

      AWS.config.update({accessKeyId: 'AKIAJJXJTSB3RUPHDTIQ', secretAccessKey: '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'});

      var sns = new AWS.SNS();
      var snsParams = {
        Message: JSON.stringify(messageJSON),
        Subject: "Message not delivered From Lambda",
        TopicArn: 'arn:aws:sns:us-west-2:451967854914:Statham-notification'
        //PhoneNumber: "+56965451609"
      };
      sns.publish(snsParams, function(errSNS, dataSNS){
        var responseSNS = "";
        if(errSNS){
          responseSNS = responseSNS + 'SENDSNSERROR: ' + errSNS + ' ';
        }
        else{
          responseSNS = responseSNS + 'DATA: ' + dataSNS + ' ';
        }
        var response = {
          statusCode: 400,
          body: JSON.stringify({
              "Error" : error,
              "SNSResponse" : responseSNS
          })
        };
        callback(null, response);
      });
    });

    req.write(postData);
    req.end();
  }
};

module.exports.receiver = (event, context, callback) => {
  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: "nice",
      input: event
    })
  };
  callback(null, response);
};

module.exports.test = (event, context, callback) => {
  AWS.config.update({accessKeyId: 'AKIAJJXJTSB3RUPHDTIQ', secretAccessKey: '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'});

  var sqs = new AWS.SQS("us-west-2");

  responseMsg.sqs = "";

  var sqsParams = {
    MessageBody: JSON.stringify(
      {"hola" : "asdf"}
      ),
    QueueUrl: 'https://sqs.us-west-2.amazonaws.com/451967854914/Statham-trunk'
  };
  sqs.sendMessage(sqsParams, function(err, data) {});

  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: "Nice"
    })
  };
  callback(null, response);
};
