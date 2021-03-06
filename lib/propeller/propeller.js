

var sys = require('sys'),
    fs = require('fs'),
    querystring = require('querystring');


var Propeller = function(config) {
    this.config = config;
    this.collection_name = config.collection_name;
    this.collections = {};
    this.fields = config.fields;
    this.re_qs = new RegExp("([^?=&]+)(=([^&]*))?", "g");
    
    // Cashe pixel in memory
    var pixelData = fs.readFileSync(__dirname + "/../../images/tracking.gif", 'binary');
    this.pixel = new Buffer(43);
    this.pixel.write(pixelData, 'binary', 0);
};

Propeller.prototype = {
    init: function(dbs, callback) {
        var self = this;
        
        this.refreshDatetime();         // set current date and hour
        setInterval(function(){
            self.refreshDatetime();
        }, 10000);                      // refresh date, hour every 10 seconds
        
        this.setupDbs(dbs, function() {
            if (callback) process.nextTick(callback);
        });
    },

    setupDbs: function(dbs, callback) {
        var dbs_complete = 0;
        for ( var db_name in dbs) {
            this.setupDb(dbs[db_name], function() {});
        }
        if (callback) process.nextTick(callback);
    },
    
    setupDb: function(db, callback) {
        var self = this;
        db.createCollection(self.collection_name, function(err, collection) {
            db.collection(self.collection_name, function(err, collection) {
                if (err != null) {
                    throw err;
                }
                self.collections[db.databaseName] = collection;
                collection.cache = {};
                if (callback) process.nextTick(callback);
            });
        });
    },

    handleTrackerRequest: function(req, res) {
        var destination = this.parseDestination(req.url);
        if (destination != false) {
            this.handleClickTrackerRequest(req, res, destination);
        } else {
            this.handleImpressionTrackerRequest(req, res);
        }
    },
    
    handleImpressionTrackerRequest: function(req, res) {
        var self = this;
        this.writePixel(res, function() {
            self.parseRequestInfo(req, function(info) {
                self.logInfo(info, 'i');
            });
        });
    },
    
    handleClickTrackerRequest: function(req, res, destination) {
        if (destination == undefined) destination = this.parseDestination(req.url);
        var self = this;
        
        this.redirect(res, destination, function() {
            if (req.method == 'HEAD') return; // Not a real request.
            self.parseRequestInfo(req, function(info) {
                self.logInfo(info, 'k');
            });
        });
    },
    
    /**
     * 
     * @param info object returned by parseRequestInfo
     * @param string inc_field 'i' for impressions 'k' for clicks
     */
    logInfo: function(info, inc_field) {
        var collection = this.collections[info['db_name']];
        if (collection == undefined) return;            // invalid database name
        
        var day = info['day'],
            hour = info['hour'],
            zone_id = info['zone_id'],
            campaign_id = info['campaign_id'],
            banner_id = info['banner_id'];
        
        var _id = [day, this.zeroFill(hour), zone_id, campaign_id, banner_id].join('|');
      
        this.logInfoCache(collection, _id, inc_field,
                {    // Update
                    '$set':{'d':day, 
                    'h':hour, 
                    'z':zone_id, 
                    'c':campaign_id, 
                    'b':banner_id}, 
                    '$inc':{'i': 0, 'k': 0}
                }
            );

    },
    
    logInfoCache : function(collection, id, inc_field, data) {
        var datetime = data['$set']['d'] + data['$set']['h'];
        var cache = collection.cache[datetime];
        if (cache == undefined) cache = collection.cache[datetime] = {};
        
        var doc = cache[id];
        if (doc == undefined) doc = cache[id] = data;
        var count = doc['$inc'][inc_field] += 1;
        
        if (count >= this.config.cache_max) {
            // copy cached $inc to data
            data['$inc'] = doc['$inc'];
            
            // reset $inc in cache
            doc['$inc'] = {'i': 0, 'k': 0};
            
            collection.update({'_id':id}, data, {'upsert':1});
        }
    },

    splitQuery: function(query) {
        var queryString = {};
        (query || "").replace(
            this.re_qs,
            function($0, $1, $2, $3) { queryString[$1] = $3; }
        );
        return queryString;
    },
    
    parseDestination: function(url) {
        var start = url.indexOf('&dest=');
        if (start == -1) return false;
        else return url.substring(start + 6);
    },
  
    parseRequestInfo: function(req, callback) {
        var self = this;
        var url = req.url;
        var end = url.indexOf('&dest=');
        var substring = url.substring(url.indexOf('?') + 1, end >= 0 ? end : undefined);
        var iter = (substring.indexOf('&||') > 0) ? substring.split('&||') : [substring];
        
        iter.forEach(function(qs) {
            var info = {};
            var queryString = self.splitQuery(qs);
    
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
                    if (isNaN(int_val)) { int_val = 0;}
                    info[item] = int_val;
                }
            }
    
            info['day'] = self.day;
            info['hour'] = self.hour;
            info['db_name'] = queryString['s'] || self.config.db_names[0];
            
            process.nextTick(function () {
                callback(info);
            });
        });
    },
    
    refreshDatetime: function() {
        var date = new Date();
        var hour = parseInt(date.getHours());
        
        if (this.hour != hour) {
            this.hour = hour;
            this.day = date.getFullYear() + '-'
                // getMonth starts at zero for January
                + this.zeroFill(date.getMonth() + 1) + '-'  
                + this.zeroFill(date.getDate());
            this.runCachedIncriments();
        }
    },
    
    runCachedIncriments: function(include_current_hour, callback) {
        var count = 0; var done = 0;
        for ( var index in this.collections) {
            var collection = this.collections[index];
            for ( var datetime in collection.cache) {
                if (include_current_hour || datetime != this.day + this.hour) {
                    var cache = collection.cache[datetime];
                    for ( var id in cache) {
                        var data = cache[id];
                        count++;
                        collection.update({'_id':id}, data, {'upsert':1},
                                function () {done++;});
                    }
                    delete collection.cache[datetime];
                }
            }
        }
        var callbackClock = function () {
            if (count == done) callback();
            else process.nextTick(callbackClock);
        };
        if (callback) callbackClock();
    },

    writePixel: function(res, callback) {
        res.writeHead(200, { 'Content-Type': 'image/gif',
                             'Content-Disposition': 'inline',
                             'Cache-Control': 'no-cache, must-revalidate',
                             'Expires': 'Sat, 26 Jul 1997 05:00:00 GMT',
                             'Content-Length': '43',
                             'Connection': 'close' });
        try {
            if (res._hasBody) res.write(this.pixel);
            res.end();
        } catch (e) {
            res.end();
            console.log(e.message);
        }
        if (callback) process.nextTick(callback);
    },
    
    redirect: function(res, destination_url, callback) {
        res.writeHead(302, {    'Location': destination_url,
                                'Cache-Control': 'no-cache, must-revalidate',
                                'Expires': 'Sat, 26 Jul 1997 05:00:00 GMT',
                                'Connection': 'close' });
        res.end();
        if (callback) process.nextTick(callback);
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
