// UNCLASSIFIED

/**
@class JSDB
@requires cluster
@requires enum
@requires mysql
*/

var 
	// globals
	TRACE = "B>",
	
	// nodejs modules
	CLUSTER = require("cluster"),

	// 3rd party bindings
	MYSQL = require("mysql");

const { Copy,Each,Log } = require("enum");

var
	JSDB = module.exports = {
		
		errors: {		//< errors messages
			noConnect: new Error("sql pool exhausted or undefined"),
			nillUpdate: new Error("nill update query"),
			unsafeQuery: new Error("unsafe queries not allowed"),
			unsupportedQuery: new Error("query not supported"),
			invalidQuery: new Error("query invalid"),
			noTable: new Error("dataset definition missing table name"),
			noDB: new Error("no database connected"),
			noLock: new Error("record lock ID missing"),
			isUnlocked: new Error("record never locked"),
			failLock: new Error("record locking failed"),
			isLocked: new Error("record already locked"),
			noExe: new Error("record execute undefined"),
			noRecord: new Error("no record found")
		},

		fetcher: () => Trace("data fetcher not configured"), //< data fetcher
		
		attrs: {		//< reserved for dataset attributes derived during config
			default:	{ 					// default dataset attributes
				sql: null, // sql connector
				query: "",  // sql query
				opts: null,	// ?-options to sql query
				unsafeok: true,  // allow/disallow unsafe queries
				trace: false,   // trace ?-compressed sql queries
				journal: true,	// attempt journally of updates to jou.table database
				ag: "", 		// default aggregator "" implies "least(?,1)"
				index: {select:"*"}, 	// data search and index
				client: "guest", 		// default client 
				track: false, 		// change journal tracking
				search: ""  // key,key, .... fulltext keys to search
			}		
		},
		
		config: function (opts, cb) {  // callback cb(sql connection)
			
			if (opts) Copy(opts,JSDB,".");
			
			//Trace("CONFIG JSDB");
			
			if (mysql = JSDB.mysql) {
				
				mysql.pool = MYSQL.createPool(mysql.opts);

				sqlThread( function (sql) {

					var ex = [  // extend sql connector with useful methods
						toQuery,
						
						// key getters
						getKeys,
						getFields,
						getTables,
						jsonKeys,
						searchKeys,
						geometryKeys,
						textKeys,
						
						// record enumerators
						build,
						run,
						relock,
						forFirst,
						forEach,
						forAll,
						thenDo,
						onEnd,
						onError,
						
						// misc
						context,
						cache,
						hawk,
						flattenCatalog,
						
						// bulk insert records
						beginBulk,
						endBulk,
						
						// job processing
						selectJob,
						deleteJob,
						updateJob,
						insertJob,
						executeJob						
					];
					
					ex.extend(sql.constructor);

					sql.query("DELETE FROM openv.locks");

					Object.defineProperties( sql.constructor.prototype, {
						ctx: {
							get: function () { 
								return this._ctx; 
							},
							set: function (ctx) { 
								Log("sql set>>>>>>>>>>>", ctx); 
								this._ctx = ctx;
							}
						},
						ds: {
							get: function () {
								return this._ctx.err;
							},
							set: function (req) {
								var 
									ctx = this._ctx,
									sql = this,
									query = ctx.where,
									emit = JSDB.emit;									

								switch ((req||0).constructor) {
									case Function:  // select
										ctx.crud = "select";
										switch (req.name) {
											case "each":
												return sql.run( ctx, null, function (err,recs) {
													recs.forEach( function (rec) {
														req(rec, me);
													});
												});

											case "clone":
											case "trace":
												return;

											case "all":
											default:
												return sql.run( ctx, null, function (err,recs) {
													req(recs, me);
												});				
										}
										break;
										
									case Array:		// insert
										ctx.crud = "insert";
										req.forEach( function (rec) {
											ctx.set = rec;
											sql.run( ctx, emit, function (err,info) {
												ctx.err = err;
											});
										});	
										break;
										
									case Object:		// update
										ctx.crud = "update";
										ctx.set = req;
										if ( query.ID ) 
											sql.run( ctx, emit, function (err,info) {
												ctx.err = err;

												if (true) {  // update change journal if enabled
													sql.hawk({Dataset:ds, Field:""});  // journal entry for the record itself
													if (false)   // journal entry for each record key being changed
														for (var key in req) { 		
															sql.hawk({Dataset:ds, Field:key});
															sql.hawk({Dataset:"", Field:key});
														}
												}
											});
										break;
										
									case Number:  // delete
										if ( query.ID ) {
											ctx.crud = "delete";
											sql.run( ctx, emit, function (err,info) {
												ctx.err = err;
											});
										}
										break;
										
									case String:		// locking / unlocking
										sql.relock(function () {  // unlocked
											switch (req) {
												case "select": break;
												case "delete": 	sql.ds = null; break;
												case "update":	sql.ds = ctx.set; break;
												case "insert":	sql.ds = [ctx.set]; break;
											}

										}, function () {  // locked
											//res( rec );
										});
										break;
								}
							}
						}
					}); 
					
					var 
						attrs = JSDB.attrs,
						dsFrom = "app",
						dsKey = "Tables_in_" + dsFrom;

					sql.query(`SHOW TABLES FROM ${dsFrom}`, function (err, recs) {
						recs.each( function (n,rec) {
							sql.searchKeys( ds = dsFrom + "." + rec[dsKey], [], function (keys) {
								var attr = attrs[ds] = {};
								for (var key in attrs.default) attr[key] = attrs.default[key];
								attr.search = keys.join(",");
							});
						});

						sql.release();

						if (cb) cb(null);						
					});
					
				});
				
			}
			
			else
				cb( new JSDB.errors.noDB );
			
			return JSDB;
		},
		
		msql: null,  //< reserved for mysql connector
		
		emit: null,  //< reserved for socketio emit
			
		thread: sqlThread,
		forEach: sqlEach,
		forAll: sqlAll,
		forFirst: sqlFirst,
		context: sqlContext
	};

function getKeys(table, type, keys, cb) {
	this.query(`SHOW KEYS FROM ${table} WHERE ?`,{Index_type:type}, function (err, recs) {
		recs.each( function (n,rec) {
			keys.push(rec.Column_name);
		});
		cb(keys);
	});
}

function getFields(table, where, keys, cb) {
	this.query( 
		where 
			? `SHOW FULL FIELDS FROM ${table} WHERE least(?,1)`
			: `SHOW FULL FIELDS FROM ${table} `, 
		
		where, function (err, recs) {
			
			recs.each( function (n, rec) {
				keys.push(rec.Field);
			});
			if (cb) cb(keys);
	});
}

function jsonKeys(table, keys, cb) {
	this.getFields(table, {Type:"json"}, keys, cb);
}

function textKeys(table, keys, cb) {
	this.getFields(table, {Type:"mediumtext"}, keys, cb);
}

function searchKeys(table, keys, cb) {
	this.getKeys(table, "fulltext", keys, cb);
}

function geometryKeys(table, keys, cb) {
	this.getFields(table, {Type:"geometry"}, keys, cb);
}

function thenDo(cb) {
	var sql = this;
	this.q.on("end", function () {
		if (cb) cb(sql);
	});
	return this;
}

function onEnd(cb) {  // on-end callback cb() and release connection
	var sql = this;
	this.q.on("end", function () {
		if (cb) cb(sql);
		sql.release();
	});
	return this;
}

function onError(cb) {  // on-error callback cb(err) and release connection
	var sql = this;
	this.q.on("error", cb);
	return this;
}

function forFirst(msg, query, args, cb) {  // callback cb(rec) or cb(null) if error
	this.q = this.query( query || "#ignore", args, function (err,recs) {  // smartTokens(query,args)
		cb( err ? null : recs[0] );
	});
	
	if (msg) msg.trace(this.q.sql);	
	return this;
}

function forEach(msg, query, args, cb) { // callback cb(rec) with each rec
	
	// smartTokens(query,args)
	this.q = this.query( query || "#ignore", args).on("result", cb);
	
	if (msg) msg.trace(this.q.sql);	
	return this;
}

function forAll(msg, query, args, cb) { // callback cb(recs) if no error
	this.q = this.query( query || "#ignore", args, function (err,recs) {
		if (!err) if(cb) cb( recs );
	})
	
	if (msg) msg.trace(this.q.sql);	
	return this;
}

function context(ctx,cb) {  // callback cb(dsctx) with a JSDB context
	var 
		sql = this,
		dsctx = {};
	
	Each(ctx, function (dskey, dsats) {
		dsctx[dskey] = new DATASET( sql, dsats );
	});
	cb(dsctx);
}

/*
Implements generic cache.  Looks for cache given opts.key and, if found, returns cached results on cb(results);
otherwse, if not found, returns results via opts.make(fetcher, opts.parms, cb).  If cacheing fails, then opts.default 
is returned.  The returned results will always contain a results.ID for its cached ID.  If a opts.default is not provided,
then the cb callback in not made.
*/

function cache( opts, cb ) {
	var sql = this;
	
	if ( opts.key )
		sql.forFirst( 
			"", 
			"SELECT ID,Results FROM app.cache WHERE least(?,1) LIMIT 1", 
			[ opts.key ], function (rec) {

			if (rec) 
				try {
					cb( Copy( JSON.parse(rec.Results), {ID:rec.ID}) );
				}
				catch (err) {
					if ( opts.default )
						cb( Copy(opts.default, {ID: 0} ) );
				}

			else
			if ( opts.make && opts.parms ) 
				opts.make( JSDB.fetcher, opts.parms, function (res) {

					if (res) 
						sql.query( 
							"INSERT INTO app.cache SET Added=now(), Results=?, ?", 
							[ JSON.stringify(res || opts.default), opts.key ], 
							function (err, info) {
								cb( Copy(res, {ID: err ? 0 : info.insertId}) );
						});

					else 
					if ( opts.default )
						cb( Copy(opts.default, {ID: 0}) );
				});

			else
			if ( opts.default )
				cb( Copy(opts.default, {ID: 0}) );
		});
	
	else
	if ( opts.default )
		cb( Copy(opts.default, {ID: 0}) );
	
}

//============== Build insert records

function beginBulk() {
	this.query("START TRANSACTION");
	this.query("SET GLOBAL sync_binlog=0");
	this.query("SET GLOBAL innodb-flush-log-at-trx-commit=0");
}

function endBulk() {
	this.query("COMMIT");
	this.query("SET GLOBAL sync_binlog=1");
	this.query("SET GLOBAL innodb-flush-log-at-trx-commit=1");
}

//=========== Job queue interface
/*
 * Job queue interface
 * 
 * select(where,cb): route valid jobs matching sql-where clause to its assigned callback cb(job).
 * execute(client,job,cb): create detector-trainging job for client with callback to cb(job) when completed.
 * update(where,rec,cb): set attributes of jobs matching sql-where clause and route to callback cb(job) when updated.
 * delete(where,cb): terminate jobs matching sql-whereJob cluase then callback cb(job) when terminated.
 * insert(job,cb): add job and route to callback cb(job) when executed.
 */

JSDB.queues = {};
	
function selectJob(where, cb) { 
/*
@method selectJob
@param {Object} req job query
@param {Function} cb callback(rec) when job departs
*
* Callsback cb(rec) for each queuing rec matching the where clause.
* >>> Not used but needs work 
 */

	// route valid jobs matching sql-where clause to its assigned callback cb(req).
	var sql = this;
	
	sql.query(
		where
		? `SELECT *,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client WHERE ${where} ORDER BY QoS,Priority`
		: `SELECT *,profiles.* FROM queues LEFT JOIN profiles ON queues.Client=profiles.Client ORDER BY QoS,Priority`
	)
	.on("error", function (err) {
		Log(err);
	})
	.on("result", function (rec) {
		cb(rec);
	});	
}

function updateJob(req, cb) { 
/*
@method updateJob
@param {Object} req job query
@param {Function} cb callback(sql,job) when job departs
*
* Adjust priority of jobs matching sql-where clause and route to callback cb(req) when updated.
* >>> Not used but needs work 
*/
	
	var sql = this;
	
	sql.selectJob(req, function (job) {
		
		cb(job.req, function (ack) {

			if (req.qos)
				sql.query("UPDATE queues SET ? WHERE ?", [{
					QoS: req.qos,
					Notes: ack}, {ID:job.ID}]);
			else
			if (req.inc)
				sql.query("UPDATE queues SET ?,Priority=max(0,min(5,Priority+?)) WHERE ?", [{
					Notes: ack}, req.inc, {ID:job.ID}]);
			
			if (req.qos) {  // move req to another qos queue
				delete JSDB.queues[job.qos].batch[job.ID];
				job.qos = req.qos;
				JSDB.queues[qos].batch[job.ID] = job;
			}
			
			if (req.pid)
				CP.exec(`renice ${req.inc} -p ${job.pid}`);				
				
		});
	});
}
		
function deleteJob(req, cb) { 
/*
@method deleteJob
@param {Object} req job query
@param {Function} cb callback(sql,job) when job departs
* >>> Not used but needs work
*/
	
	var sql = this;
	sql.selectJob(req, function (job) {
		
		cb(sql,job, function (ack) {
			sql.query("UPDATE queues SET Departed=now(), Age=(now()-Arrived)/3600e3, Notes=concat(Notes,'stopped') WHERE ?", {
				Task:job.task,
				Client:job.client,
				Class:job.class,
				QoS:job.qos
			});

			delete JSDB.queues[job.qos].batch[job.priority];
			
			if (job.pid) CP.exec("kill "+job.pid); 	// kill a spawned req
		});
	});
}

function insertJob(job, cb) { 
/*
@method insertJob
@param {Object} job arriving job
@param {Function} cb callback(sql,job) when job departs

Adds job to the specified (client,class,qos,task) queue.  A departing job will execute the supplied 
callback cb(sql,job) on a new sql thread (or spawn a new process if job.cmd provided).  The job
is regulated by its job.rate [s] (0 disables regulation). If the client's job.credit has been exhausted, the
job is added to the queue, but not to the regulator.  Queues are periodically monitored to store 
billing information.  
 */
	function cpuavgutil() {				// compute average cpu utilization
		var avgUtil = 0;
		var cpus = OS.cpus();
		
		cpus.each(function (n,cpu) {
			idle = cpu.times.idle;
			busy = cpu.times.nice + cpu.times.sys + cpu.times.irq + cpu.times.user;
			avgUtil += busy / (busy + idle);
		});
		return avgUtil / cpus.length;
	}
	
	function regulate(job,cb) {		// regulate job (spawn if job.cmd provided)
			
		var queue = JSDB.queues[job.qos];	// get job's qos queue
		
		if (!queue)  // prime the queue if it does not yet exist
			queue = JSDB.queues[job.qos] = new Object({
				timer: 0,
				batch: {},
				rate: job.qos,
				client: {}
			});
			
		var client = queue.client[job.client];  // update client's bill
		
		if ( !client) client = queue.client[job.client] = new Object({bill:0});
		
		client.bill++;

		var batch = queue.batch[job.priority]; 		// get job's priority batch
		
		if (!batch) 
			batch = queue.batch[job.priority] = new Array();

		batch.push( Copy(job, {cb:cb, holding: false}) );  // add job to queue
		
		if ( !queue.timer ) 		// restart idle queue
			queue.timer = setInterval(function (queue) {  // setup periodic poll for this job queue

				var job = null;
				for (var priority in queue.batch) {  // index thru all priority batches
					var batch = queue.batch[priority];

					job = batch.pop(); 			// last-in first-out

					if (job) {  // there is a departing job 
//Log("job depth="+batch.length+" job="+[job.name,job.qos]);

						if (job.holding)  // in holding / stopped state so requeue it
							batch.push(job);
									   
						else
						if (job.cmd) {	// this is a spawned job so spawn and hold its pid
							job.pid = CP.exec(
									job.cmd, 
									  {cwd: "./public/dets", env:process.env}, 
									  function (err,stdout,stderr) {

								job.err = err || stderr || stdout;

								if (job.cb) job.cb( job );  // execute job's callback
							});
						}
					
						else  			// execute job's callback
						if (job.cb) job.cb(job);

						break;
					}
				}

				if ( !job ) { 	// an empty queue goes idle
					clearInterval(queue.timer);
					queue.timer = null;
				}

			}, queue.rate*1e3, queue);
	}

	var 
		sql = this;
	
	if (job.qos)  // regulated job
		sql.query(  // insert job into queue or update job already in queue
			"INSERT INTO app.queues SET ? ON DUPLICATE KEY UPDATE " +
			"Departed=null, Work=Work+1, State=Done/Work*100, Age=(now()-Arrived)/3600e3, ?", [{
				// mysql unique keys should not be null
				Client: job.client || "",
				Class: job.class || "",
				Task: job.task || "",
				QoS: job.qos || 0,
				// others 
				State: 0,
				Arrived	: new Date(),
				Departed: null,
				Marked: 0,
				Name: job.name,
				Age: 0,
				Classif : "",
				//Util: cpuavgutil(),
				Priority: job.priority || 0,
				Notes: job.notes,
				Finished: 0,
				Billed: 0,
				Funded: job.credit ? 1 : 0,
				Work: 1,
				Done: 0
			}, {
				Notes: job.notes,
				Task: job.task || ""
			}
		], function (err,info) {  // increment work backlog for this job

			//Log([job,err,info]);
			
			if (err) 
				return Log(err);
			
			job.ID = info.insertId || 0;
			
			if (job.credit)				// client still has credit so place it in the regulators
				regulate( Copy(job,{}) , function (job) { // clone job and provide a callback when job departs
					sqlThread( function (sql) {  // callback on new sql thread
						cb(sql,job);

						sql.query( // reduce work backlog and update cpu utilization
							"UPDATE app.queues SET Age=now()-Arrived,Done=Done+1,State=Done/Work*100 WHERE ?", [
							// {Util: cpuavgutil()}, 
							{ID: job.ID} //jobID 
						]);
	
						sql.release();
						/*
						sql.query(  // mark job departed if no work remains
							"UPDATE app.queues SET Departed=now(), Notes='finished', Finished=1 WHERE least(?,Done=Work)", 
							{ID:job.ID} //jobID
						);
						*/
					});
				});
		});

	else  { // unregulated so callback on existing sql thread
		job.ID = 0;
		cb(sql, job);
	}
}
	
function executeJob(req, exe) {

	function flip(job) {  // flip job holding state
		if ( queue = JSDB.queues[job.qos] ) 	// get job's qos queue
			if ( batch = queue.batch[job.priority] )  // get job's priority batch
				batch.each( function (n, test) {  // matched jobs placed into holding state
					if ( test.task==job.task && test.client==job.client && test.class==job.class )
						test.holding = !test.holding;
				});
	}
	
	var sql = req.sql, query = req.query;
	
	sql.query("UPDATE ??.queues SET Holding = NOT Holding WHERE ?", {ID: query.ID}, function (err) {
		
		if ( !err )
			flip();
	});
}

function flattenCatalog(flags, catalog, limits, cb) {
/**
 @method flattenCatalog
 Flatten entire database for searching the catalog
 * */
	
	function flatten( sql, rtns, depth, order, catalog, limits, cb) {
		var table = order[depth];
		
		if (table) {
			var match = catalog[table];
			var filter = cb.filter(match);
			
			var quality = " using "+ (filter ? filter : "open")  + " search limit " + limits.records;
			
			Trace("CATALOG "+table+quality+" RECS "+rtns.length, sql);
		
			var query = filter 
					? "SELECT SQL_CALC_FOUND_ROWS " + match + ",ID, " + filter + " FROM ?? HAVING Score>? LIMIT 0,?"
					: "SELECT SQL_CALC_FOUND_ROWS " + match + ",ID FROM ?? LIMIT 0,?";
					
			var args = filter
					? [table, limits.score, limits.records]
					: [table, limits.records];

			sql.query( query, args,  function (err,recs) {
				
				if (err) {
					rtns.push( {
						ID: rtns.length,
						Ref: table,
						Name: "error",
						Dated: limits.stamp,
						Searched: 0,
						Link: (table + ".db").tag("a",{href: "/" + table + ".db"}),
						Content: err+""
					} );

					flatten( sql, rtns, depth+1, order, catalog, limits, cb );
				}
				else 
					sql.query("select found_rows()")
					.on('result', function (stat) {
						
						recs.each( function (n,rec) {						
							rtns.push( {
								ID: rtns.length,
								Ref: table,
								Name: `${table}.${rec.ID}`,
								Dated: limits.stamp,
								Quality: recs.length + " of " + stat["found_rows()"] + quality,
								Link: table.tag("a",{href: "/" + table + ".db?ID=" + rec.ID}),
								Content: JSON.stringify( rec )
							} );
						});

						flatten( sql, rtns, depth+1, order, catalog, limits, cb );
					});
			});	
		}
		else
			cb.res(rtns);
	}

	var 
		sql = this,
		rtns = [];
		/*limits = {
			records: 100,
			stamp: new Date()
			//pivots: flags._pivot || ""
		};*/
		
	flatten( sql, rtns, 0, FLEX.listify(catalog), catalog, limits, {
		res: cb, 

		filter: function (search) {
			return ""; //Builds( "", search, flags);  //reserved for nlp, etc filters
	} });
}

function sqlThread(cb) {  // callback cb(sql) with a sql connection

	function dummyConnector() {
		var
			This = this,
			err = JSDB.errors.noDB;

		this.query = function (q,args,cb) {
			Trace("NODB "+q);
			if (cb)
				cb(err);
			else
			if (args && args.constructor == Function)
				args(err);

			return This;
		};

		this.on = function (ev, cb) {
			return This;
		};

		this.sql = "DUMMY SQL CONNECTOR";

		this.release = function () {
			return This;
		};

		this.createPool = function (opts) {
			return null;
		};
	}

	var 
		mysql = JSDB.mysql;

	if (mysql)
		if ( mysql.pool) 
			mysql.pool.getConnection( function (err,sql) {
				if (err) 
					Log(JSDB.errors.noConnect, {
						sqlpool: err,
						total: mysql.pool._allConnections.length ,
						free: mysql.pool._freeConnections.length,
						queue: mysql.pool._connectionQueue.length
					});

				else 
					cb( sql );
			});

		else
		if ( sql = MYSQL.createConnection(mysql.opts) ) 
			cb( sql );

		else
			Log(JSDB.errors.noConnect);

	else 
		Log(JSDB.errors.noConnect);
}

function sqlEach(trace, query, args, cb) {
	sqlThread( function (sql) {
		sql.forEach( trace, query, args, function (rec) {
			cb(rec, sql);
		}).onEnd( );
	});
}

function sqlAll(trace, query, args, cb) {
	sqlThread( function (sql) {
		sql.forAll( trace, query, args, function (recs) {
			cb(recs, sql);
			sql.release();
		});
	});
}

function sqlFirst(trace, query, args, cb) {
	sqlThread( function (sql) {
		sql.forFirst(trace, query, args, function (rec) {
			cb(rec, sql);
			sql.release();
		});
	});
}

function sqlContext(ctx, cb) {
	sqlThread( function (sql) {
		sql.context( ctx, function (dsctx) {
			cb(dsctx, sql);
			sql.release();
		});
	});
}

function getTables(db, cb) {
	var 
		key = `Tables_in_${db}`,
		tables = [];
				  
	this.query( "SHOW TABLES FROM ??", [db], function (err, recs) {
		if ( !err ) {
			recs.forEach( function (rec) {
				//tables[ rec[key] ] = db;
				tables.push( rec[key] );
			});
			cb( tables );
		}
	});
}

function build(opts) {
	var
		sql = this,
		escape = MYSQL.escape,
		escapeId = MYSQL.escapeId,
		ex = ""; 
	
	switch ( opts.crud ) {
		case "select":
			if ( pivot = opts.pivot ) {
				var 
					where = opts.where || {},
					slash = "_",
					nodeID = where.NodeID || "root",
					index = opts.index = (nodeID == "root") 
						? {
							"NodeID:": `group_concat(DISTINCT ID SEPARATOR '${slash}')`,
							"NodeCount:": "count(ID)",
							"leaf:": "false",
							"expandable:": "true",
							"expanded:": "false"
						}
						: {
							"NodeID:": `'${nodeID}`,
							"NodeCount:": "true",
							"leaf:": "true",
							"expandable:": "true",
							"expanded:": "false",
							"ID:$": nodeID
						};

				if (nodeID == "root") {
					opts.group = pivot;
					delete where.NodeID;
				}

				else 
					opts.group = null;

				Log(index);
			}
			else
			if ( browse = opts.brows ) {
				var	
					slash = "_", 
					where = opts.where || {},
					nodeID = where.NodeID,
					nodes = nodeID ? nodeID.split(slash) : [],
					pivots = browse.split(","),
					group = opts.group = (nodes.length >= pivots.length)
						? pivots.concat(["ID"])
						: pivots.slice(0,nodes.length+1),
					name = pivots[nodes.length] || "concat('ID',ID)",
					path = group.join(",'"+slash+"',"),
					index = opts.index = {
						"NodeID:": `group_concat(DISTINCT ${path})`,
						"NodeCount:": "count(ID)",
						"path:": '/tbd',
						"read:": "1",
						"write:": "1",
						"group:": "v1",
						"locked:": "1"
					};

				index[name+":"] = `cast(${name} AS char)`;

				delete where.NodeID;
				nodes.forEach( function (node) {
					where[ pivots[n] || "ID" ] = node;
				});
			}
						
			if ( index = sql.toQuery(opts.index) )
				ex += sql.format("SELECT SQL_CALC_FOUND_ROWS ?", index);
			else
				ex += "SELECT SQL_CALC_FOUND_ROWS *" ;
			
			if ( from = opts.from )
				ex += sql.format(" FROM ??", from );
			
			if ( where = sql.toQuery(opts.where) )
				ex += sql.format(" WHERE least(?,1)", where );
			
			if ( having = sql.toQuery(opts.having) )
				ex += sql.format(" HAVING least(?,1)", having );
			
			if ( sort = opts.sort ) 
				try {
					var by = [];
					sort.forEach( function (opt) {
						var key = escapeId(opt.property);
						by.push(`${key} ${opt.direction}`);
					});
					by = by.join(",");
					if (by) ex += ` ORDER BY ${by}`;
				}
				catch (err) {
				}
			
			if ( group = opts.group ) 
				ex += sql.format(" GROUP BY " + escapeId( group.split(",") ));
			
			if ( order = opts.order )
				ex += sql.format(" ORDER BY " + escapeId( order.split(",") ));
			
			if (limit = opts.limit)
				ex += sql.format(" LIMIT ?", limit);

			if (offset = opts.offset)
				ex += sql.format(" OFFSET ?", offset);
			
			break;
			
		case "update":
			if ( from = opts.from )
				ex += sql.format("UPDATE ??" , from);
			
			if ( set = opts.set )
				ex += sql.format(" SET ?" , set);
			
			if ( where = sql.toQuery(opts.where) )
				ex += sql.format(" WHERE least(?,1)", where);
			
			else
				ex = "#UPDATE NO WHERE";
			
			break;
			
		case "delete":
			if ( from = opts.from )
				ex += sql.format("DELETE FROM ??" , from);
			
			if ( where = sql.toQuery(opts.where) )
				ex += sql.format(" WHERE least(?,1)", where);
			
			else
				ex = "#DELETE NO WHERE";
			
			break;
			
		case "insert":
			if ( from = opts.from )
				ex += sql.format("INSERT INTO ??" , from);
			
			if ( set = sql.toQuery(opts.set) ) 
				ex += sql.format(" SET ?", set);
			
			else
				ex += sql.format(" () values ()", []);
			
			break;
	}
	
	Log( ex );
	return ex;
}

function run(ctx, emit, cb) {
	if ( ex = this.build(ctx) ) 
		if ( ctx.lock ) {
			sql.ctx = ctx;
			sql.relock( function () {  // sucessfully unlocked
				switch (ctx.crud) {
					case "select": break;
					case "delete": 	sql.ds = null; break;
					case "update":	sql.ds = ctx.set; break;
					case "insert":	sql.ds = [ctx.set]; break;
				}
				cb( sql.ctx.err, null );
			}, function () {  // sucessfully locked
				cb( sql.ctx.err, null );
				//res( rec );
			});
		}

		else
			this.query( ex, [], function (err, info) {

				cb( err, info );

				if ( 0 && emit && !err && ctx.client ) { // Notify other clients of change
					Log("emitting", ctx);
					emit( ctx.crud, {
						path: "/"+ctx.from+".db", 
						body: ctx.set, 
						ID: ctx.where.ID, 
						from: ctx.client
					});	
				}

			});
}

function hawk(log) {  // journal changes 
	var sql = this;
	
	sql.query("SELECT * FROM openv.hawks WHERE least(?,Power)", log)
	.on("result", function (hawk) {
		sql.query(
			"INSERT INTO openv.journal SET ? ON DUPLICATE KEY UPDATE Updates=Updates+1",
			Copy({
				Hawk: hawk.Hawk,  	// moderator
				Power: hawk.Power, 	// moderator's level
				Updates: 1 					// init number of updates made
			}, log), function (err) {
				Log("journal", err);
		});
	});
}
		
function relock(unlockcb, lockcb) {  			// lock-unlock record 
	var 
		sql = this,
		ctx = this.ctx,
		ID = ctx.query.ID,
		lockID = {Lock:`${ctx.from}.${ID}`, Client:ctx.client};

	if (ID)
		sql.query(  // attempt to unlock a locked record
			"DELETE FROM openv.locks WHERE least(?)", 
			lockID, 
			function (err,info) {

			if (err)
				ctx.err = JSDB.errors.failLock;

			else
			if (info.affectedRows) {  // unlocked so commit queued queries
				unlockcb();
				sql.query("COMMIT");  
			}

			else 
			if (lockcb)  // attempt to lock this record
				sql.query(
					"INSERT INTO openv.locks SET ?",
					lockID, 
					function (err,info) {

					if (err)
						ctx.err = JSDB.errors.isLocked;

					else
						sql.query( "START TRANSACTION", function (err) {  // queue this transaction
							lockcb();
						});
				});	

			else  // record was never locked
				ctx.err = JSDB.errors.isUnlocked;

		});

	else
		ctx.err = JSDB.errors.noLock;
}

function toQuery(query) {
	for (var key in query) 
		return new QUERY(query);
	
	return null;
}

function QUERY(query) {
	for (var key in query) this[key] = query[key];
}

QUERY.prototype.toSqlString = function () {
	
	function build(key, val, cb) {
		var 
			esc = escape(val),
			op = key.substr(-1),
			id = escapeId( key.substr(0,key.length-op.length) );
		
		switch ( op ) {
			case "*":
				var vals = esc.split(",");
				return `${id} BETWEEN ${vals[0]} AND ${vals[1]}`;

			case "%":
				return `${id} LIKE ${esc}`;

			case "<":
			case ">":
			case "!":
				return id + op + "=" + esc;

			case "$":
					
				op = key.substr(-2);
				id = escapeId( key.substr(0,key.length-op.length) );
				
				switch ( op ) {
					case "/$":
						return `MATCH(Search) AGAINST(${esc}) AS ${id}`;
					case "^$":
						return `MATCH(Search) AGAINST(${esc} IN BINARY MODE) AS ${id}`;
					case "|$":
						return `MATCH(Search) AGAINST(${esc} IN QUERY EXPANSION) AS ${id}`;
					case "<$":
					case ">$":
						return id + op.substr(0,1) + esc;
					case ":$":
						return `instr( '_${val}_' , concat('_' , ${id} , '_'))`;
					default:
						return "";
				}
				
			case "/":
				return `MATCH(${id}) AGAINST(${esc})` ;
			case "^":
				return `MATCH(${id}) AGAINST(${esc} IN BINARY MODE)` ;
			case "|":
				return `MATCH(${id}) AGAINST(${esc} WITH QUERY EXPANSION)` ;
				
			case ":":

				var 
					jsons = (val+"").split("$"),
					exprs = [];

				if ( jsons.length>1) {   // have a json extract id:=json expression
					jsons.forEach( function (expr,n) {
						if ( n ) exprs.push( escape( "$"+expr ) );
					});

					exprs = exprs.join(",");
					return `json_extract( ${jsons[0]}, ${exprs} ) AS ${id}` ;
				}

				else   // have an sql askey:=sql expression
					return `${val} AS ${id}` ;

			default:
				id = escapeId( key );
				return `${id}=${esc}` ;
		}
		
	}
	
	var 
		escape = MYSQL.escape,
		escapeId = MYSQL.escapeId,
		rtn = [];

	Each(this, function (key, val) {
		if (val) 
			rtn.push( build( key, val ) );
	});
	
	return rtn.join(", ");
}

function Trace(msg,sql) {
	TRACE.trace(msg,sql);
}
