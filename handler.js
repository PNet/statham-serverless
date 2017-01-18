'use strict';
var https = require('https');
var url = require('url');
var AWS = require('aws-sdk');
var EventEmitter = require("events").EventEmitter;
var responseMsg = new EventEmitter();

module.exports.sendMessage = (event, context, callback) => {
  var enviar = false;
  var messageJSON = JSON.parse(event.body);
  
  if(!messageJSON.try){
      messageJSON.try = 0;
      enviar = true;
  }
  else if(messageJSON.try < 5){
      messageJSON.try = messageJSON.try + 1;
      enviar = true;
  }
  else{
      //eliminar mensaje y notificar
  }
  
  if(enviar){
    var postData = JSON.stringify(messageJSON.body);
    
    var urlDest = url.parse(messageJSON.url);

    var options = {
      hostname: urlDest.host,
      port: 443,
      path: urlDest.pathname,
      method: messageJSON.method,
      headers: {
        'Content-Type' : 'application/json',
        'Content-Length': postData.length
      }
    };

    var req = https.request(options, (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        console.log(`BODY: ${chunk}`);
        responseMsg.data = JSON.parse(chunk);
        responseMsg.emit('success');
      });
      res.on('end', () => {
        console.log('No more data in response.');
        responseMsg.error = "No more data";
        responseMsg.emit('error');
      });
    });

    req.on('error', (e) => {
      console.log(`problem with request: ${e.message}`);
      responseMsg.error = `problem with request: ${e.message}`;
      responseMsg.emit('error');
    });

    req.write(postData);
    req.end();

    responseMsg.on('success', function () {
      var response = {
        statusCode: 200,
        body: JSON.stringify({
            "Success" : responseMsg.data
            })
      };

      callback(null, response);
    });
    responseMsg.on('error', function () {
        
      AWS.config.update({accessKeyId: 'AKIAJJXJTSB3RUPHDTIQ', secretAccessKey: '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'});
    
      var sqs = new AWS.SQS("us-west-2");
      
      responseMsg.sqs = "";
      
      var sqsParams = {
        MessageBody: JSON.stringify(messageJSON),
        QueueUrl: 'https://sqs.us-west-2.amazonaws.com/451967854914/Statham-trunk'
      };
      sqs.sendMessage(sqsParams, function(err, data) {
        if (err) {
          console.log('STORESQSERROR:', err);
          responseMsg.sqs = responseMsg.sqs + 'STORESQSERROR: ' + err + ' ';
          responseMsg.emit('sqsResponse');
        }
        else{
          console.log(data);
          responseMsg.sqs = responseMsg.sqs + 'DATA: ' + data + ' ';
          responseMsg.emit('sqsResponse');
        }
      });
      
      responseMsg.on('sqsResponse', function(){
        var response = {
          statusCode: 400,
          body: JSON.stringify({
              "Error" : responseMsg.error,
              "SQSResponse" : responseMsg.sqs
          })
        };
        callback(null, response);
      });
    });
  }
  else{
    var response = {
      statusCode: 400,
      body: JSON.stringify({
        "body" : "Mensaje no enviado: supera numero maximo de try (5)"
      })
    };
    callback(null, response);
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