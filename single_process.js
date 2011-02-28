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

// Load Modules
var http = require('http'),
    mongo = require('mongodb');
var Propeller = require('propeller/propeller').Propeller;

// Create DB
db = new mongo.Db(config.db_name, 
        new mongo.Server(config.mongodb_host, config.mongodb_port, {}), {});

db.addListener("error", function(error) {
  console.log("Error connecting to mongo -- perhaps it isn't running?");
});

// Open Persistant Connection
db.open(function(p_db) {
    var propeller = new Propeller(config);
    propeller.init(db, function() {
        var server = http.createServer(function (req, res) {
            try {
                propeller.handleTrackerRequest(req, res);
            } catch (e) {
                propeller.handleError(req, res, e);
            }
        });
        
        server.listen(tracker_port);
        console.log('Tracker running on port ' + tracker_port);
    });
});
