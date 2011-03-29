/**
 *  Script that loads propeller and configs, and starts server on a single
 *  process.  
 */

// Make sure lib folder is in require path
require.paths.unshift(__dirname + '/lib');
require.paths.unshift(__dirname);

try {
    // Check for custom config
    var config = require('propeller_config').config;
} catch (e) {
    // Fall back to distrution config
    var config = require('propeller_config.dist').config;
}
// Load Config
var tracker_port = config['tracker_port'];
var num_nodes = config['nodes'];

// Load Modules
var http = require('http'),
    mongo = require('mongodb');
var Propeller = require('propeller/propeller').Propeller;

var db_names = config.db_names.concat();

function setup(dbs_done, dbs_left, callback) {
    if (dbs_left.length != 0) {
        var db_name = dbs_left.shift();
        // Create DB
        db = new mongo.Db(db_name, 
                new mongo.Server(config.mongodb_host, config.mongodb_port, {}), {});
        
        // Handle Connection Error
        db.addListener("error", function(error) {
                console.log("Error connecting to mongo -- perhaps it isn't running?");
            });
        
        // Get database
        db.open(function(p_db) {
            dbs_done[db_name] = db;
            setup(dbs_done, dbs_left, callback);
        });
    
    } else {
        callback(dbs_done);
    }
}

// Open Persistant Connections
setup({}, db_names, function(dbs) {
    var propeller = new Propeller(config);
    propeller.init(dbs, function() {
        var server = http.createServer(function (req, res) {
            try {
                propeller.handleTrackerRequest(req, res);
            } catch (e) {
                propeller.handleError(req, res, e);
            }
        });
        
        var nodes = require("multi-node").listen({
           port:            tracker_port,
           nodes:           num_nodes,
           masterListen:    false,
           restartChildren: true
        }, server);

        if (nodes.isMaster) {
            console.log('Starting ' + num_nodes + ' trackers listening  on port ' + tracker_port + '.');
        } else {
            console.log('Tracker up');
        }
    });
});

