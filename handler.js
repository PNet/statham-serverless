  'use strict';
  var https             = require('https');
  var url               = require('url');
  var AWS               = require('aws-sdk');
  var sleep             = require('sleep');
  var EventEmitter      = require("events").EventEmitter;
  var responseMsg       = new EventEmitter();
  var Key_Id            = 'AKIAJJXJTSB3RUPHDTIQ';
  var secretAccessKey   = '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';
  AWS.config.update({accessKeyId: Key_Id, secretAccessKey: secretAccessKey});
  var sns               = new AWS.SNS();

// Code Message

var fetch_request_message = function(event){
  var messageJSON;
  if(event.Records){
    messageJSON = JSON.parse(event.Records[0].Sns.Message);
  }
  else{
    messageJSON = JSON.parse(event.body);
    messageJSON.source = event.headers.Origin;
  }
  return messageJSON;
}

var validate_tries_message = function(messageJSON, callback){
  if(!messageJSON.tries)
    messageJSON.tries = 0;
  messageJSON.tries += 1;

  if(messageJSON.tries > 5)
    error_message_to_email(messageJSON, function(response){
      callback(response);
    });
  else
    send_message(messageJSON, function(response){
      callback(response);
    });
}

var mail_message_generator = function(messageJSON){
  var message = 
  `Attempted to send the message five times but the destination couldn't be reached.
  Details:
  Method: ${messageJSON.method}
  URL destination: ${messageJSON.url}
  Source: ${messageJSON.source}
  Destination path: ${messageJSON.dest}
  Body: ${JSON.stringify(messageJSON.body)}
  ${messageJSON.error}`;
  return message;
}

var error_message_to_email = function(messageJSON, callback){
  var message = mail_message_generator(messageJSON);
  var snsParams = serialize_sns(
    message,
    "A message reached the maximum number of sending attempts",
    'arn:aws:sns:us-west-2:451967854914:Statham-mailer'
  );

  sns.publish(snsParams, function(errSNS, dataSNS){
    var responseSNS = get_response(errSNS, dataSNS);
    var response = make_json_response(400,{
      "SNSResponse" : responseSNS
    });
    callback(response);
  });
}

// Code SNS

var post_data = function(messageJSON){
  return JSON.stringify(messageJSON.body);
}

var serialize_options = function(messageJSON){
  var postData = post_data(messageJSON);    
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
  return options;
}

var serialize_sns = function(message, subject, topic){
  var snsParams = {
    Message: message,
    Subject: subject,
    TopicArn: topic
  };
  return snsParams;
}

var get_response = function(errSNS, dataSNS){
  var responseSNS = "";
  if(errSNS){
    responseSNS = 'Send SNS error: ' + errSNS;
  }
  else{
    responseSNS = 'Data: ' + dataSNS;
  }
  return responseSNS;
}

var make_json_response = function(statusCode,body){
  var response = {
    statusCode: statusCode,
    body: JSON.stringify(body)
  };
  return response;
}

var send_message = function(messageJSON, callback){
  if(messageJSON.tries > 1) sleep(10000);
  var postData = post_data(messageJSON);
  var options = serialize_options(messageJSON);

  var req = https.request(options, (res) => {
    var data = "";
    res.setEncoding('utf8');
    res.on('data', (chunk) => {
      data += chunk;
    });
    res.on('end', () => {
      var response = make_json_response(200,{
        "Success" : JSON.parse(data)
      });
      callback(response);
    });
  });

  req.on('error', (e) => {
    var error = `ERROR: ${e.message}`;
    messageJSON.error = error;

    var snsParams = serialize_sns(
      JSON.stringify(messageJSON), 
      "Message not delivered From Lambda", 
      'arn:aws:sns:us-west-2:451967854914:Statham-notification');

    sns.publish(snsParams, function(errSNS, dataSNS){
      var responseSNS = get_response(errSNS, dataSNS);

      var response = make_json_response(400,{
        "Error" : error,
        "SNSResponse" : responseSNS
      });
      callback(response);
    });
  });
  req.write(postData);
  req.end();
}

module.exports.sendMessage = (event, context, callback) => {
  var messageJSON = fetch_request_message(event);
  validate_tries_message(messageJSON, function(response){
    callback(null, response);  
  });  
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