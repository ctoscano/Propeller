

var sys = require('sys'),
    fs = require('fs'),
    querystring = require('querystring');


var Propeller = function(config) {
    this.collection_name = config.collection_name;
    this.fields = config.fields;
    
    // Cashe pixel in memory
    var pixelData = fs.readFileSync(__dirname + "/../../images/tracking.gif", 'binary');
    this.pixel = new Buffer(43);
    this.pixel.write(pixelData, 'binary', 0);
};

Propeller.prototype = {
    init: function(db, callback) {
        this.setupDb(db, function() {
          callback();
        });
    },

    setupDb: function(db, callback) {
        var self = this;
        db.createCollection(self.collection_name, function(err, collection) {
            db.collection(self.collection_name, function(err, collection) {
                if (err != null) {
                    throw err;
                }
                self.collection = collection;
                callback();
            });
        });
    },

    handleTrackerRequest: function(req, res) {
        var self = this;
        this.writePixel(res, function() {
            self.parseRequestInfo(req, function(info) {
                self.logInfo(info);
            });
        });
    },
    
    logInfo: function(info) {
        var day = info['day'],
            hour = info['hour'],
            zone_id = info['zone_id'],
            campaign_id = info['campaign_id'],
            banner_id = info['banner_id'];
        
        var _id = [day, hour, zone_id, campaign_id, banner_id].join('|');
        var inc_field = 'i'; // i: impression
        var inc = {};
        inc[inc_field] = 1;
      
        this.collection.update({'_id':_id},  
                {    // Update
                    '$set':{'d':day, 
                    'h':hour, 
                    'z':zone_id, 
                    'c':campaign_id, 
                    'b':banner_id}, 
                    '$inc':inc
                },
                {    // Options
                    'upsert':1
                }
            );

    },

    splitQuery: function(query) {
        var queryString = {};
        (query || "").replace(
            new RegExp("([^?=&]+)(=([^&]*))?", "g"),
            function($0, $1, $2, $3) { queryString[$1] = querystring.unescape($3.replace(/\+/g, ' ')); }
        );
        return queryString;
    },
  
    parseRequestInfo: function(req, callback) {
        var info = {};
        var queryString = this.splitQuery(req.url.split('?', 2)[1]);

        info['zone_id'] = queryString['zoneid'];
        info['campaign_id'] = queryString['campaignid'];
        info['banner_id'] = queryString['bannerid'];
        info['debug'] = queryString['debug'];
        
        for(var item in info) {
            var value = info[item];
            if (value == undefined) {
                // zero out missing ids
                info[item] = 0;
            } else {
                // convert string to ints
                var int_val = parseInt(info[item]); 
                if (int_val == NaN) { int_val = 0;}
                info[item] = int_val;
            }
        }

        var date = new Date();
        
        info['day'] = date.getFullYear() + '-'
                        // getMonth starts at zero for January
                        + this.zeroFill(date.getMonth() + 1) + '-'  
                        + this.zeroFill(date.getDate());
        info['hour'] = parseInt(date.getHours());
        
        callback(info);
    },

    writePixel: function(res, callback) {
        res.writeHead(200, { 'Content-Type': 'image/gif',
                             'Content-Disposition': 'inline',
                             'Content-Length': '43',
                             'Connection': 'close' });
        try {
            res.end(this.pixel);
        } catch (e) {
            res.end();
            console.log(e.message);
        }
        callback();
    },

    handleError: function(req, res, e) {
        res.writeHead(500, {});
        res.write("Server error");
        res.end();

        if (e.stack != undefined) { e.stack = e.stack.split('\n'); }
        e.url = req.url;
        
        try {
            sys.log(JSON.stringify(e, null, 2));
        } catch (e2) {
            sys.log(e);
            sys.log(e2);
        }
    },
    
    zeroFill: function ($number) {
        if ($number >= 10) {
            return '' + $number;
        } else {
            return '0' + $number;
        }
    }

};

exports.Propeller = Propeller;
