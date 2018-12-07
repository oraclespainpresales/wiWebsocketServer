'use strict';

// Module imports
var restify = require('restify-clients')
  , queue = require('block-queue')
  , express = require('express')
  , http = require('http')
  , bodyParser = require('body-parser')
  , kafka = require('kafka-node')
  , async = require('async')
  , _ = require('lodash')
  , log = require('npmlog-ts')
  , commandLineArgs = require('command-line-args')
  , getUsage = require('command-line-usage')
  , cors = require('cors')
;

// Instantiate classes & servers
const PROCESSNAME  = "WEDO Industry - Websocket Server"
    , VERSION      = "v1.0"
    , AUTHOR       = "Carlos Casares <carlos.casares@oracle.com>"
    , PROCESS      = "PROCESS"
    , WEBSOCKET    = "WEBSOCKET"
    , REST         = "REST"
    , KAFKA        = "KAFKA"
    , CONNECTED    = "CONNECTED"
    , DISCONNECTED = "DISCONNECTED"
    , wsURI        = '/socket.io'
    , restURI      = '/event/:eventname'
;
var restapp     = express()
  , restserver  = http.createServer(restapp)
;

// ************************************************************************
// Main code STARTS HERE !!
// ************************************************************************

log.stream = process.stdout;
log.timestamp = true;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info("","Uncaught Exception: " + err);
  log.info("","Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info("","Caught interrupt signal");
  log.info("","Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

// Initialize input arguments
const optionDefinitions = [
  { name: 'dbhost', alias: 'd', type: String },
  { name: 'pinginterval', alias: 'i', type: Number },
  { name: 'pingtimeout', alias: 't', type: Number },
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];

const sections = [
  {
    header: PROCESSNAME + " " + VERSION,
    content: 'Websocket Server fir WEDO Industry events'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'dbhost',
        typeLabel: '{underline ipaddress:port}',
        alias: 'd',
        type: String,
        description: 'DB setup server IP address/hostname and port'
      },
      {
        name: 'pinginterval',
        typeLabel: '{underline milliseconds}',
        alias: 'i',
        type: Number,
        description: 'Ping interval in milliseconds for event clients'
      },
      {
        name: 'pingtimeout',
        typeLabel: '{underline milliseconds}',
        alias: 't',
        type: Number,
        description: 'Ping timeout in milliseconds for event clients'
      },
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
]

const options = commandLineArgs(optionDefinitions);

const valid =
  options.help ||
  (
    options.dbhost &&
    options.pinginterval &&
    options.pingtimeout
  );

if (!valid) {
  console.log(getUsage(sections));
  process.exit(-1);
}

log.level = (options.verbose) ? 'verbose' : 'info';

const pingInterval = options.pinginterval || 25000
    , pingTimeout  = options.pingtimeout  || 60000
    , RESTPORT = 10001
    , PROTOCOL = 'https://'
    , DEMOZONESETUPURI = '/ords/pdb1/wedoindustry/demozone/zone'
    , EVENTHUBSETUPURI = '/ords/pdb1/wedoindustry/setup/eventhub'
;

// REST engine initial setup
restapp.use(bodyParser.urlencoded({ extended: true }));
restapp.use(bodyParser.json());
restapp.use(cors());

var client = restify.createJsonClient({
  url: PROTOCOL + options.dbhost,
  rejectUnauthorized: false,
  headers: {
    "content-type": "application/json"
  }
});

var demozones = _.noop();
var servers = [];

// KAFKA STUFF BEGIN
var kafkaSetup     = {}
  , Consumer       = kafka.Consumer
  , kafkaClient    = _.noop()
  , kafkaConsumer  = _.noop()
  , kafkaCnxStatus = DISCONNECTED;
;

function startKafka(cb) {
  kafkaClient = new kafka.Client(kafkaSetup.zookeeper, "RETAIL", {sessionTimeout: 1000});
  kafkaClient.zk.client.on('connected', () => {
    kafkaCnxStatus = CONNECTED;
    log.info(KAFKA, "Server connected!");
  });
  kafkaClient.zk.client.on('disconnected', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.info(KAFKA, "Server disconnected!");
  });
  kafkaClient.zk.client.on('expired', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.info(KAFKA, "Server disconnected!");
  });

  async.eachSeries(demozones, (demozone,next) => {
    var d = kafkaSetup.topic.replace('{demozone}', demozone.id.toLowerCase()) ;
    log.verbose(KAFKA, "Starting consumer in topic '%s'", d);
    kafkaConsumer = new Consumer(
      kafkaClient, [ { topic: d, partition: 0 } ], { autoCommit: true }
    );

    kafkaConsumer.on('message', data => {
      log.verbose(KAFKA, "Incoming message on topic '%s', payload: %s", data.topic, data.value);
      try {
        var message = JSON.parse(data.value);
        if (!message.payload) {
          log.error(KAFKA, "Message does not contain 'payload'. Ignoring");
          return;
        }
        if (!message.demozone) {
          log.error(KAFKA, "Message does not contain 'demozone'. Ignoring");
          return;
        }
        message.demozone = message.demozone.toUpperCase();
        var server = _.find(servers, { 'demozone': message.demozone });
        if (!server) {
          log.error(KAFKA, "Incoming demozone '%s' not registered. Ignoring", message.demozone);
          return;
        }
        if (!message.eventname) {
          log.error(KAFKA, "Message does not contain 'eventname'. Ignoring");
          return;
        }
        if (server.clients > 0) {
          log.verbose(WEBSOCKET,"Sending event to %s (%s, %d): %j", message.eventname, message.demozone, server.port, message.payload);
          server.io.sockets.emit(message.eventname, message.payload);
        }
      } catch(e) {
        log.error(KAFKA, "Error parsing incoming message. Not a valid JSON object");
      }
    });

    kafkaConsumer.on('ready', () => {
      log.error(KAFKA, "Consumer ready");
    });

    kafkaConsumer.on('error', err => {
      log.error(KAFKA, "Error initializing KAFKA consumer: " + err.message);
    });

    next();
  }, (err) => {
    if (typeof(cb) == 'function') cb();
  });
}

function stopKafka(cb) {
  if (kafkaClient) {
    kafkaClient.close(() => {
      if (typeof(cb) == 'function') cb();
    });
  } else {
    if (typeof(cb) == 'function') cb();
  }
}
// KAFKA STUFF END

async.series([
  function(next) {
    log.info(PROCESS, "%s - %s", PROCESSNAME, VERSION);
    log.info(PROCESS, "Author - %s", AUTHOR);
    next();
  },
  function(next) {
    log.verbose(PROCESS, "Retrieving current demozones from %s", PROTOCOL + options.dbhost + DEMOZONESETUPURI);
    client.get(DEMOZONESETUPURI, function(err, req, res, obj) {
      if (err) {
        next(err.message);
      }
      var jBody = JSON.parse(res.body);
      if (!jBody.items || jBody.items.length == 0) {
        next("No demozones found. Aborting.");
      } else {
        demozones = jBody.items;
        next();
      }
    });
  },
  function(next) {
    log.verbose(PROCESS, "Retrieving EventHub setup");
    client.get(EVENTHUBSETUPURI, function(err, req, res, obj) {
      if (err) {
        next(err.message);
      }
      var jBody = JSON.parse(res.body);
      kafkaSetup.zookeeper = jBody.zookeeperhost;
      kafkaSetup.topic     = jBody.eventtopic;
      next();
    });
  },
  function(next) {
    log.verbose(KAFKA, "Connecting to Zookeper host at %s...", kafkaSetup.zookeeper);
    startKafka(next);
  },
  function(next) {
    async.eachSeries(demozones, (demozone,callback) => {
      var d = {
        demozone: demozone.id,
        name: demozone.name,
        port: parseInt(demozone.proxyport) + 10300,
        clients: 0
      };
      d.app = express();
      d.server = http.createServer(d.app);
      d.io = require('socket.io')(d.port, {
        serveClient: false,
        pingInterval: pingInterval,
        pingTimeout: pingTimeout
      });
      log.info(WEBSOCKET,"Created WS server for demozone '" + d.demozone + "' at port: " + d.port);
      d.io.on('connection', (socket) => {
        d.clients++;
        log.info(d.demozone,"Socket connected. Current clients: %d", d.clients);
        socket.conn.on('heartbeat', () => {
          log.verbose(d.demozone,'heartbeat');
        });
        socket.on('disconnect', () => {
          d.clients--;
          log.info(d.demozone,"Socket disconnected. Current clients left: %d", d.clients);
        });
        socket.on('error', err => {
          log.error(d.demozone,"Error: " + err);
        });
      });
      servers.push(d);
      callback();
    }, (err) => {
      next();
    });
  }
/**
  function(next) {
    restserver.listen(RESTPORT, function() {
      log.info(REST,"REST server running on http://localhost:" + RESTPORT + restURI);
      next();
    });
  }
**/
], function(err, results) {
  if (err) {
    log.error("", err.message);
    process.exit(2);
  }
});
/**
restapp.post(restURI, function(req,res) {
  res.status(204).send();
//  log.verbose("","Request: " + JSON.stringify(req.body));
  if (req.params.eventname) {
    // find out the demozone
    if (!req.body[0].payload.data.data_demozone) {
      log.error(REST, "No {payload.data.data_demozone} structure found in payload: " + JSON.stringify(req.body));
      return;
    }
    var demozone  = req.body[0].payload.data.data_demozone.toUpperCase();
    var server = _.find(servers, { 'demozone': demozone });
    if (server) {
//      var namespace = demozone.toLowerCase() + "," + req.params.eventname;
      var namespace = req.params.eventname;
      log.verbose(WEBSOCKET,"Sending %d event(s) to %s (%s, %d)", req.body.length, namespace, demozone, server.port);
      server.io.sockets.emit(namespace, req.body);
    } else {
      log.error(REST, "Request received for a demozone not registered (" + demozone + ")");
    }
  }
});
**/
