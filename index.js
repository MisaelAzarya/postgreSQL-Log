var pg = require('pg');
var LogicalReplication = require('pg-logical-replication');
var PluginTestDecoding = LogicalReplication.LoadPlugin('output/test_decoding');

function ins(pgClient, table, data){
    return new Promise((resolve, reject) => {
    var qry = "INSERT INTO "+table+" VALUES ("+data+")";
    pgClient.query(qry, function(err, results) {
        if (err) {
        console.error(err);
        return reject(err);
        }
        resolve(results);
    })
    })
}
  
function updt(pgClient, table, data, param_key, data_key){
    return new Promise((resolve, reject) => {
    pgClient.query("UPDATE "+table+" SET "+data+" WHERE "+param_key+" = '"+data_key+"'", function(err, results) {
        if (err) {
        console.error(err);
        return reject(err);
        }
        resolve(results);
    })
    })
}
  
function dlt(pgClient, table, param_key, data_key){
    return new Promise((resolve, reject) => {
    pgClient.query("DELETE FROM "+table+" WHERE "+param_key+" = '"+data_key+"'", function(err, results) {
        if (err) {
        console.error(err);
        return reject(err);
        }
        resolve(results);
    })
    })
}

// connection untuk source
var connInfo = {
	host: 'localhost',
	port: 5432,
	user: 'postgres',
	password: '1234',
	database: 'supersample'
}

// connection ke db tujuan
const pgConString = "postgres://postgres:1234@localhost:5432/replica"
var clientpg = new pg.Client(pgConString);
clientpg.connect();

//Initialize with last LSN value
var lastLsn = null;

var stream = (new LogicalReplication(connInfo))
	.on('data', function(msg) {
		lastLsn = msg.lsn || lastLsn;

		var log = (msg.log || '').toString('utf8');
		try {
			var logs = PluginTestDecoding.parse(log);
			console.log(logs);
			
			var data = [];
            var params = [];
            var table = "table1";
            if(logs.trx==null){ // ketika log menampilkan data
                var param_key = logs.data[0].name;
                var data_key = logs.data[0].value;

				for(var i=0;i<logs.data.length;i++){
					params.push(logs.data[i].name);
					data.push("'"+logs.data[i].value+"'");
                }

                if(logs.action=="INSERT"){
                    var promise = ins(clientpg, table, data);
                    promise.then(function(result){
                        console.log('ID = '+logs.data[0].value+' Inserted')
                    });
                }else if(logs.action=="UPDATE"){
                    data = [];
                    for(var i=1;i<logs.data.length;i++){
                        data.push(logs.data[i].name + " = '" + logs.data[i].value + "'");
                    }
                    var promise = updt(clientpg, table, data, param_key, data_key);
                    promise.then(function(result){
                        console.log('ID = '+logs.data[0].value+' Updated')
                    });
                }else if(logs.action=="DELETE"){
                    var promise = dlt(clientpg, table, param_key, data_key);
                    promise.then(function(result){
                        console.log('ID = '+logs.data[0].value+' Deleted')
                    });
                }
                
			}
			//TODO: DO SOMETHING. eg) replicate to other dbms(pgsql, mysql, ...)
		} catch (e) {
			console.trace(log, e);
		}
	}).on('error', function(err) {
		console.trace('Error #2', err);
		setTimeout(proc, 1000);
	});

function proc() {
	stream.getChanges('test_slot', lastLsn, {
		includeXids: false, //default: false
		includeTimestamp: false, //default: false
	}, function(err) {
		if (err) {
			console.trace('Logical replication initialize error', err);
			setTimeout(proc, 1000);
		}
	});
};
proc();