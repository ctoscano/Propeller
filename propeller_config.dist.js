/**
 *     Configuration options. 
 */ 

config = {
    tracker_port:       8888,
    
    mongodb_host:       '127.0.0.1',
    mongodb_port:       27017,
    
    db_names:           ['www'],
    collection_name:    'log_summary',
    
    nodes:              4,
    
    cache_max:          10,
    
    // Fields not used yet
    fields: [
                {
                    db_name:        'z',
                    url_name:       'zoneid'
                },{
                    db_name:        'c',
                    url_name:       'campaignid'
                },{
                    db_name:        'b',
                    url_name:       'bannerid'
                }
            ]
};

exports.config = config;
