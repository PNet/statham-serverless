'use strict';
var https = require('https');
var url = require('url');
var AWS = require('aws-sdk');
var sleep = require('sleep');
var EventEmitter = require("events").EventEmitter;
var responseMsg = new EventEmitter();
var sns = new AWS.SNS();
var messageJSON; 
var Key_Id          = 'AKIAJJXJTSB3RUPHDTIQ'
var secretAccessKey = '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'

function AWS(){
  AWS.config.update({accessKeyId: Key_Id, secretAccessKey: secretAccessKey});
}

function format_message_sns(event){
  var message;
  if(event.Records){
      message = JSON.parse(event.Records[0].Sns.Message);
  }
  else{
      message = JSON.parse(event.body);
      message.source = event.headers.Origin;
  }
  return message;
}

module.exports.sendMessage = (event, context, callback) => {
  var messageJSON = format_message_sns(event);
  
  if(!messageJSON.tries)
    messageJSON.tries = 0;

  messageJSON.tries += 1;

  if(messageJSON.tries > 5){

    var message = `Attempted to send the message five times but the destination couldn't be reached.\n
    Details:\n
                   Method: ${messageJSON.method}\n
                   URL destination: ${messageJSON.url}\n
                   Source: ${messageJSON.source}\n
                   Destination path: ${messageJSON.dest}\n

                   Body: ${JSON.stringify(messageJSON.body)}\n

                   ${messageJSON.error}\n
                   `;

    var snsParams = {
      Message: message,
      Subject: "A message reached the maximum number of sending attempts",
      TopicArn: 'arn:aws:sns:us-west-2:451967854914:Statham-mailer'
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
            "SNSResponse" : responseSNS
        })
      };
      callback(null, response);
    });

  }
  else{
    if(messageJSON.tries > 1) sleep(10000); //fixed to 10 seconds but can be replaced (replace timeout of the function too)

    var postData = JSON.stringify(messageJSON.body);

    var urlDest = url.parse(messageJSON.url);

    messageJSON.dest = urlDest.pathname;

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

      messageJSON.error = error;

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
