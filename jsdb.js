// UNCLASSIFIED

/**
@class DB
@requires cluster
@requires enum
@requires mysql
*/

var	
	// globals
	ENV = process.env,
	
	// nodejs modules
	CLUSTER = require("cluster"),

	// 3rd party bindings
	MYSQL = require("mysql");

function Trace(msg,req,fwd) {	//< execution tracing
	"db".trace(msg,req,fwd);
}

const { Copy,Each,Log,isFunction,isString,isArray,isEmpty,isDate } = require("enum");
const { escape, escapeId } = MYSQL;

const { mysql } = DB = module.exports = {
	config: (opts,cb) => {  // callback cb(sql connection)
		if (opts) Copy(opts,DB,".");

		//Trace("CONFIG DB");

		/*
			mysql.connection = MYSQL.createConnection(mysql.opts);
			mysql.connection.connect();
			// nonpooled connector permits connection reuse within other queries, but they become responsible to release() the connection
		*/

		var pool = mysql.pool = MYSQL.createPool(mysql);

		// may be useful to test thread depth to protect against DOS attacks
		pool.on("acquire", () => mysql.threads ++ );
		pool.on("release", () => mysql.threads -- );

		sqlThread( sql => {

			[						
				// key getters
				getKeys,
				//getTypes,
				getFields,
				getTables,
				getJsons,
				getSearchables,
				getGeometries,
				getTexts,

				// query processing
				Query,

				// record enumerators
				relock,
				forFirst,
				forEach,
				forAll,
				//thenDo,
				//onEnd,
				//onError,

				// misc
				reroute,
				serialize,
				context,
				cache,
				hawk,
				flattenCatalog,

				/*
				//escapeing,
				function escape(arg) { return MYSQL.escape(arg); },
				function escapeId(key) { return MYSQL.escapeId(key); }, 
				*/

				// bulk insert records
				beginBulk,
				endBulk,

				// job processing
				selectJob,
				deleteJob,
				updateJob,
				insertJob,
				executeJob						
			].Extend(sql.constructor);

			sql.query("DELETE FROM openv.locks");

			Object.defineProperties( sql.constructor.prototype, {
				ctx: {
					get: function () { 
						return this._ctx; 
					},
					set: function (ctx) { 
						//Log("sql set>>>>>>>>>>>", ctx); 
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
							query = ctx.where;

						switch ((req||0).constructor) {
							case Function:  // select
								ctx.crud = "select";
								switch (req.name) {
									case "each":
										return sql.Query( ctx, null, (err,recs) => recs.forEach( rec => req(rec, me) ) );

									case "clone":
									case "trace":
										return;

									case "all":
									default:
										return sql.Query( ctx, null, (err,recs) => req(recs, me) );				
								}
								break;

							case Array:		// insert
								ctx.crud = "insert";
								req.forEach( rec => {
									ctx.set = rec;
									sql.Query( ctx, null, (err,info) => {
										ctx.err = err;
									});
								});	
								break;

							case Object:		// update
								ctx.crud = "update";
								ctx.set = req;
								if ( query.ID ) 
									sql.Query( ctx, null, (err,info) => {
										ctx.err = err;
									});
								break;

							case Number:  // delete
								if ( query.ID ) {
									ctx.crud = "delete";
									sql.Query( ctx, null, (err,info) => {
										ctx.err = err;
									});
								}
								break;

							case String:		// locking / unlocking
								sql.relock( () => {  // unlocked
									switch (req) {
										case "select": break;
										case "delete": 	sql.ds = null; break;
										case "update":	sql.ds = ctx.set; break;
										case "insert":	sql.ds = [ctx.set]; break;
									}

								}, () => {  // locked
									//res( rec );
								});
								break;
						}
					}
				}
			}); 

			var 
				attrs = DB.attrs;

			sql.query(`SHOW TABLES FROM app`, (err,recs) => {
				recs.forEach( rec => {
					sql.getSearchables( ds = "app." + rec.Tables_in_app, keys => {
						var attr = attrs[ds] = {};
						for (var key in attrs.default) attr[key] = attrs.default[key];
						attr.search = keys.join(",");
					});
				});

				if (cb) cb(null);						
			});

		});
	},
	
	queues: { 	//< reserve for job queues
	},

	reroute: {  //< db.table -> db.table translators to protect or reroute tables
	},

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

	probeSite: (url,opt) => { throw new Error("data probeSite not configured"); }, //< data probeSite

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
			//track: false, 		// change journal tracking
			search: ""  // key,key, .... fulltext keys to search
		}		
	},

	/**
	@cfg {Object} 
	@member TOTEM
	MySQL connection options {host, user, pass, sessions} or nul l to disable
	*/		
	mysql: { //< connection options
		// connection options
		host: ENV.MYSQL_HOST || "MYSQL_HOST undefiend",
		user: ENV.MYSQL_USER || "MYSQL_USER undefined",
		password: ENV.MYSQL_PASS || "MYSQL_PASS undefined",
		connectionLimit: 50,	// max number to create "at once" - whatever that means
		acquireTimeout: 600e3,	// ms timeout during connection acquisition - whatever that means
		connectTimeout: 600e3,	// ms timeout during initial connect to mysql server
		queueLimit: 0,  						// max concections to queue (0=unlimited)
		waitForConnections: true,		// queue when no connections are available and limit reached
		
		// reserved for ...
		threads: 0, 	// connection threads
		pool: null		// connector
		/*
		function dummyConnector() {
			var
				This = this,
				err = DB.errors.noDB;

			this.query = function (q,args,cb) {
				Trace("NODB "+q);
				if (cb)
					cb(err);
				else
				if ( args && isFunction(args) )
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
		} */		
	},
	
	//emitter: null,  //< reserved for socketio emitter

	thread: sqlThread,
	forEach: sqlEach,
	forAll: sqlAll,
	forFirst: sqlFirst,
	context: sqlContext
};
	
//============ key access

function getKeys(table, query, keys, cb) {
	this.query(
		(where = whereify({"=":query}))
			? `SHOW KEYS FROM ?? WHERE least(${where},1)`
			: "SHOW KEYS FROM ?? ",
		
		table, (err,recs) => {
			
			if (!err)
				if ( keys.push )
					recs.forEach( rec => keys.push(rec.Column_name) );
			
				else
					recs.forEach( rec => keys[rec.Field] = rec.Index_type );
			
			cb(keys);
	});
}

function getFields(table, query, keys, cb) {
	this.query( 
		(where = whereify({"=":query}))
			? `SHOW FULL FIELDS FROM ?? WHERE least(${where},1)`
			: "SHOW FULL FIELDS FROM ?? ", 
		
		table, (err, recs) => {
			
			if (!err) 
				if ( keys.push )
					recs.forEach( (rec,n) => keys.push(rec.Field) );
					
				else
					recs.forEach( rec => keys[rec.Field] = rec.Type );
			
			cb(keys);
	});
}

function getJsons(table, cb) {
	this.getFields(table, {Type:"json"}, [], cb);
}

function getTexts(table, cb) {
	this.getFields(table, {Type:"mediumtext"}, [], cb);
}

function getSearchables(table, cb) {
	this.getKeys(table, {Index_type:"fulltext"}, [], cb);
}

function getGeometries(table, cb) {
	this.getFields(table, {Type:"geometry"}, [], cb);
}

function getTables(db, cb) {
	var 
		key = `Tables_in_${db}`,
		tables = [];
				  
	this.query( "SHOW TABLES FROM ??", [db], (err,recs) => {
		if ( !err ) {
			recs.forEach( rec => {
				//tables[ rec[key] ] = db;
				tables.push( rec[key] );
			});
			cb( tables );
		}
	});
}

function context(ctx,cb) {  // callback cb(dsctx) with a DB context
	var 
		sql = this,
		dsctx = {};
	
	Each(ctx, function (dskey, dsats) {
		dsctx[dskey] = new DATASET( sql, dsats );
	});
	cb(dsctx);
}

//============== Record cacheing and bulk record inserts
 
function cache( opts, cb ) {
/*
Implements generic cache.  Looks for cache given opts.key and, if found, returns cached results on cb(results);
otherwse, if not found, returns results via opts.make(probeSite, opts.parms, cb).  If cacheing fails, then opts.default 
is returned.  The returned results will always contain a results.ID for its cached ID.  If a opts.default is not provided,
then the cb callback in not made.
*/
	var 
		sql = this,
		probeSite = DB.probeSite,
		defRec = {ID:0};
	
	if ( opts.key )
		sql.forFirst( 
			"", 
			"SELECT ID,Results FROM app.cache WHERE least(?,1) LIMIT 1", 
			[ opts.key ], rec => {

			if (rec) 
				try {
					cb( Copy( JSON.parse(rec.Results), {ID:rec.ID}) );
				}
				catch (err) {
					if ( opts.default )
						cb( Copy(opts.default, defRec ) );
				}

			else
			if ( opts.make ) 
				if (probeSite)
					opts.make( probeSite.tag(opts.parms || {}), ctx => {

					if (ctx) 
						sql.query( 
							"INSERT INTO app.cache SET Added=now(), Results=?, ?", 
							[ JSON.stringify(ctx || opts.default), opts.key ], 
							function (err, info) {
								cb( Copy(ctx, {ID: err ? 0 : info.insertId}) );
						});

					else 
					if ( opts.default )
						cb( Copy(opts.default, {ID: 0}) );
				});
				
				else
					cb( defRec );

			else
			if ( opts.default )
				cb( Copy(opts.default, defRec) );
		});
	
	else
	if ( opts.default )
		cb( Copy(opts.default, defRec) );
	
}

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

/*
Job queue interface
 
select(where,cb): route valid jobs matching sql-where clause to its assigned callback cb(job).
execute(client,job,cb): create detector-trainging job for client with callback to cb(job) when completed.
update(where,rec,cb): set attributes of jobs matching sql-where clause and route to callback cb(job) when updated.
delete(where,cb): terminate jobs matching sql-whereJob cluase then callback cb(job) when terminated.
insert(job,cb): add job and route to callback cb(job) when executed.
*/

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
	.on("error", err => {
		Log(err);
	})
	.on("result", rec => {
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
				delete DB.queues[job.qos].batch[job.ID];
				job.qos = req.qos;
				DB.queues[qos].batch[job.ID] = job;
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
				QoS:job.qos  // [secs]
			});

			delete DB.queues[job.qos].batch[job.priority];
			
			if (job.pid) CP.exec("kill "+job.pid); 	// kill a spawned req
		});
	});
}

function insertJob(job, cb) { 
/*
@method insertJob
@param {Object} job arriving job
@param {Function} cb callback(job) when job departs

Adds job to the specified (client,class,qos,task) queue.  A departing job will execute the supplied 
callback cb(job) or spawn a new process if job.cmd provided.  The job is regulated by its job.rate [s] 
(0 disables regulation). If the client's job.credit has been exhausted, the job is added to the queue, 
but not to the regulator.  Queues are periodically monitored to store billing information.  
 */
	function regulate(job,cb) {		// regulate job (spawn if job.cmd provided)
			
		var queue = DB.queues[job.qos];	// get job's qos queue
		
		if ( !queue )  // prime the queue if it does not yet exist
			queue = DB.queues[job.qos] = new Object({	// reserve queue for requested qos polling level
				timer: 0,	// reserved for setInterval
				batch: {},	// each batch reserved for different priorities
				rate: job.qos,  // in seconds
				client: {}	// reserved for client billing information
			});
			
		// update client's bill (default client = "guest")
		
		var client = queue.client[job.client || "guest"]; 
		
		if ( !client ) client = queue.client[job.client || "guest"] = new Object({count:1, until: job.until || 1, start: new Date( job.start || "1950") , end: new Date( job.end || "2050" ) });
		
		//Log(">>>>client", client);
		// access priority batch for this job 
		
		var batch = queue.batch[job.priority || 0]; 		// get job's priority batch (default priority = 0)
		
		if ( !batch ) 
			batch = queue.batch[job.priority || 0] = new Array();

		batch.push( Copy(job, {cb:cb}) );  // add job to queue
		
		if ( !queue.timer ) 		// restart idle queue
			queue.timer = setInterval( queue => {  // setup periodic poll for this job queue

				var job = null;
				for (var priority in queue.batch) {  // index thru all priority batches
					var batch = queue.batch[priority];

					if ( job = batch.pop() ) {  // remove job from the queue (last-in first-out )
//Log("job depth="+batch.length+" job="+[job.name,job.qos]);

						var 
							now = new Date(),
							client = queue.client[ job.client || "guest" ];
						
						if ( now >= client.start && now <= client.end ) {	// within run window
							if ( client.count < client.until )  {	// can run more times so put it back on the queue
								batch.push(job);
								client.count++;
							}

							if ( jobcb = job.cb )	// has a callback so run it
								if ( isString(jobcb) )  // this is a child job so spawn it and hold its pid
									job.pid = CP.exec( jobcb, {cwd: "./public", env:process.env}, (err,stdout,stderr) => {
										job.err = err || stderr || stdout;
									});

								else  // execute job's callback
									jobcb(job);
						}
						
						else
						if ( now < client.start )  // cant start yet so put it back on the queue
							batch.push(job);
						
					}
				}

				if ( !job ) { // queue empty so it goes idle
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
				Client: job.client || "guest",
				Class: job.class || "",
				Task: job.task || "",
				QoS: job.qos || 0,  // [secs]
				// others 
				State: 0,
				Arrived	: new Date(),
				Departed: null,
				Marked: 0,
				Name: job.name,
				Age: 0,
				Classif : "",
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
		], (err,info) => {  // increment work backlog for this job

			//Log("insert job", job,err,info);
			
			if (err) 
				cb( null ); 	// signal error
			
			else {
				job.ID = info.insertId || 0;

				if ( job.credit )				// client still has credit so place it in the regulator
					regulate( Copy(job,{}) , job => { // clone job and provide a callback when job departs
						sqlThread( sql => {  // start new sql thread to run job and save metrics
							cb( sql, job );

							sql.query( // reduce work backlog 
								"UPDATE app.queues SET Age=now()-Arrived,Done=Done+1,State=Done/Work*100 WHERE ?", 
								{ID: job.ID} 
							);

							sql.query( // charge client
								"UPDATE openv.profiles SET Charge=Charge+1,Credit=Credit-1 WHERE ?", 
								{Client: job.client} 
							);
						});
					});
				
				else
					cb(null); 	// signal error
			}
		});

	else  { // unregulated so callback on existing sql thread
		job.ID = 0;
		cb(sql, job);
	}
}
	
function executeJob(req, exe) {

	function flip(job) {  // flip job holding state
		if ( queue = DB.queues[job.qos] ) 	// get job's qos queue
			if ( batch = queue.batch[job.priority] )  // get job's priority batch
				batch.forEach( test => {  // matched jobs placed into holding state
					if ( test.task==job.task && test.client==job.client && test.class==job.class )
						test.holding = !test.holding;
				});
	}
	
	var sql = req.sql, query = req.query;
	
	sql.query("UPDATE ??.queues SET Holding = NOT Holding WHERE ?", {ID: query.ID}, err => {
		
		if ( !err )
			flip();
	});
}

//================= catalog interface

function flattenCatalog(flags, catalog, limits, cb) {
/**
 @method flattenCatalog
 Flatten entire database for searching the catalog
 
 Need to rework using serialize
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

			sql.query( query, args,  (err,recs) => {
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
						recs.forEach( rec => {						
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
	
	// need to revise this to use serialize logic
	flatten( sql, rtns, 0, FLEX.listify(catalog), catalog, limits, {
		res: cb, 

		filter: function (search) {
			return ""; //Builds( "", search, flags);  //reserved for nlp, etc filters
	} });
}

//================= record enumerators

function forFirst(msg, query, args, cb) {  // callback cb(rec) or cb(null) on error
	var q = this.query( query || "#ignore", args, (err,recs) => {  
		if ( err ) 
			cb( null );
		
		else
			cb( recs[0] || null );
	});
	if (msg) Trace( `${msg} ${q.sql}`, this);	
	return q;
}

function forEach(msg, query, args, cb) { // callback cb(rec) with each rec - no cb if errror
	q = this.query( query || "#ignore", args).on("result", rec => cb(rec) );
	if (msg) Trace( `${msg} ${q.sql}`, this);	
	return q;
}

function forAll(msg, query, args, cb) { // callback cb(recs) of cb(null) on error
	var q = this.query( query || "#ignore", args, (err,recs) => {
		if ( err ) 
			cb( null );
		
		else
			cb( recs );
	});
	if (msg) Trace( `${msg} ${q.sql}`, this);	
	return q;
}

function sqlThread(cb) {  // callback cb(sql) with a sql connection
	cb(mysql.pool);
		/*
		if ( mysql.pool ) 
			mysql.pool.getConnection( (err,sql) => {
				if (err) 
					Log( DB.errors.noConnect, {
						error: err,
						total: mysql.pool._allConnections.length ,
						free: mysql.pool._freeConnections.length,
						queue: mysql.pool._connectionQueue.length,
						config: mysql
					});

				else {
					cb( sql );
					sql.release();
				}
			});
		
		else
		if ( sql = MYSQL.createConnection(mysql.opts) ) {
			cb( sql );
			//sql.release();
		}
		else
			Log(DB.errors.noConnect);	
		*/
}

function sqlEach(trace, query, args, cb) {
	sqlThread( sql => sql.forEach( trace, query, args, rec => cb(rec, sql) ) );
}

function sqlAll(trace, query, args, cb) {
	sqlThread( sql => sql.forAll( trace, query, args, recs => cb(recs, sql) ) );
}

function sqlFirst(trace, query, args, cb) {
	sqlThread( sql => sql.forFirst(trace, query, args, rec => cb(rec, sql) ) );
}

function sqlContext(ctx, cb) {
	sqlThread( sql => sql.context( ctx, dsctx => cb(dsctx, sql) ) );
}

//================== db journalling

function Query(ctx, emitter, cb) {
	
	function run(ex,cb) {
		if (true || opts.trace) Trace(ex);

		if ( ctx.lock ) {		// process form lock/unlock queries
			sql.ctx = ctx;
			sql.relock( () => {  // sucessfully unlocked
				switch (ctx.crud) {
					case "select": break;
					case "delete": 	sql.ds = null; break;
					case "update":	sql.ds = ctx.set; break;
					case "insert":	sql.ds = [ctx.set]; break;
				}
				
				cb( sql.ctx.err, null );
			}, () => {  // sucessfully locked
				cb( sql.ctx.err, null );
				//res( rec );
			});
		}

		else	// process standard queries
			sql.query( ex, [], (err, recs) => {
				cb( err, recs );

				if ( !err )
					if ( emitter && ctx.client ) { // Notify other clients of change
						Log("emitting", ctx);
						emitter( ctx.crud, {
							path: "/"+ctx.from+".db", 
							body: ctx.set, 
							ID: ctx.where.ID, 
							from: ctx.client
						});	
					}
			});
	}
	
	function Select(index) {
		var
			ex = MYSQL.format(`SELECT SQL_CALC_FOUND_ROWS ${index} FROM ??`, from);

		if ( join = opts.join )
			ex += " " + join + " ";

		if ( where = whereify(opts.where) )
			ex += MYSQL.format(` WHERE least(${where},1)` );

		if ( having = whereify(opts.having) )
			ex += MYSQL.format(` HAVING least(${having},1)` );

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
			ex += MYSQL.format(" GROUP BY " + escapeId( group.split(",") ));

		if ( order = opts.order )
			ex += MYSQL.format(" ORDER BY " + escapeId( order.split(",") ));

		if (limit = opts.limit)
			ex += MYSQL.format(" LIMIT ?", limit);

		if (offset = opts.offset)
			ex += MYSQL.format(" OFFSET ?", offset);
		
		return ex;
	}
	
	function Update() {
		var
			ex = MYSQL.format("UPDATE ??" , from);

			if ( set = opts.set )
				ex += MYSQL.format(" SET ?" , set);

			if ( where = whereify(opts.where) )
				ex += MYSQL.format( ` WHERE least(${where},1)` );

			else
				ex = "#UPDATE NO WHERE";
		
		return ex;		
	}
	
	function Delete() {
		var
			ex = MYSQL.format("DELETE FROM ??" , from);

		if ( where = whereify(opts.where) )
			ex += MYSQL.format( ` WHERE least(${where},1)` );

		else
			ex = "#DELETE NO WHERE";

		return ex;
	}
	
	function Insert() {
		var 
			ex = MYSQL.format("INSERT INTO ??" , from),
			set = opts.set || {};
		
		delete set.ID;
		delete set.id;
		
		if ( isEmpty(set) ) 
			ex += MYSQL.format(" () values ()", []);
		else
			ex += MYSQL.format(" SET ?", set);

		return ex;
	}		

	function indexify(query, cb) {	// callback cb( "index list", [store, ...] )
		function fix( key, escape, json ) {
			return key.parseOP( /(.*?)(\$)(.*)/, key => escapeId(key), (lhs,rhs,op) => {
				if (lhs) {
					jsons.push( json );
					
					var 
						keys = rhs.split(","),
						idx = [];
					
					keys.forEach( (key,n) => { 
						if ( key )
							idx.push( escape( n ? key : op+key) );
					});
						
					return idx.length 
						? `json_extract(${escapeId(lhs)}, ${idx.join(",")} )`
						: escapeId(lhs);
				}

				else
					return escapeId(rhs);
			});
		}

		var 
			jsons = [],
			takes = [],
			drops = [];
		
		for ( var lhs in query ) {
			if ( rhs = query[lhs] ) 
				takes.push( fix(rhs,escape,lhs) + " AS " + lhs );
			
			else
			if ( lhs.indexOf("%") >= 0 )
				drops.push( lhs );
		
			else
				takes.push( lhs );
		}

		if ( drops.length ) {	// drop specified fields 
			drops.forEach( (skip,n) => drops[n] = "Field NOT LIKE " + escape(skip) );

			sql.query( "SHOW FIELDS FROM ?? WHERE "+drops.join(" AND "), from, (err,recs) => {
				recs.forEach( rec => takes.push( rec.Field ) );
				cb( takes.join(",") || "*", jsons );
			});
		}

		else	// table all specified fields
			cb( takes.join(",") || "*", jsons );
	}

	var
		sql = this,
		opts = ctx,
		from = reroute( opts.from, opts );	
	
	switch ( opts.crud ) {
		case "select":
			if ( pivot = opts.pivot ) {
				var 
					where = opts.where || {},
					slash = "_",
					nodeID = where.NodeID || "root",
					index = opts.index = (nodeID == "root") 
						? {
							Node: pivot,
							ID: `group_concat(DISTINCT ID SEPARATOR '${slash}') AS ID`,
							Count: "count(ID) AS Count",
							leaf: "false AS leaf",
							expandable: "true AS expandable",
							expanded: "false AS expanded"
						}
						: {
							Node: pivot,
							ID: `'${nodeID} AS ID`,
							Count: "1 AS Count",
							leaf: "true AS leaf",
							expandable: "true AS expandable",
							expanded: "false AS expanded"
							//ID: `$${nodeID} AS ID`
						};

				if (nodeID == "root") {
					opts.group = pivot;
					delete where.NodeID;
				}

				else 
					opts.group = null;

				//Log( "jsdb piv", index);
			}

			else
			if ( browse = opts.browse ) {
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
						Node: browse,
						ID: `group_concat(DISTINCT ${path}) AS D`,
						Count: "count(ID) AS Count",
						path: '/tbd AS path',
						read: "1 AS read",
						write: "1 AS write",
						group: "'v1' AS group",
						locked: "1 AS locked"
					};

				index[name+":"] = `cast(${name} AS char)`;

				delete where.NodeID;
				nodes.forEach( function (node) {
					where[ pivots[n] || "ID" ] = node;
				});
			}

			indexify( opts.index, (index,jsons) => run( Select( index ), (err,recs) => {
				if (!err) 
					jsons.forEach( key => {
						recs.forEach( rec => {
							rec[key] = JSON.parse( rec[key] );
						});
					});

				cb( err, recs );
			}) );

			break;

		case "update":
			run( Update(), cb );
			break;

		case "delete":
			run( Delete(), cb );
			break;

		case "insert":
			run( Insert(), cb );
			break;
	}

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
			}, log), err => {
				Log("journal", err);
		});
	});
}

//================ form entry 

function relock(unlockcb, lockcb) {  //< lock-unlock record during form entry
	var 
		sql = this,
		ctx = this.ctx,
		ID = ctx.query.ID,
		lockID = {Lock:`${ctx.from}.${ID}`, Client:ctx.client};

	if (ID)
		sql.query(  // attempt to unlock a locked record
			"DELETE FROM openv.locks WHERE least(?)", 
			lockID, (err,info) => {

			if (err)
				ctx.err = DB.errors.failLock;

			else
			if (info.affectedRows) {  // unlocked so commit queued queries
				unlockcb();
				sql.query("COMMIT");  
			}

			else 
			if (lockcb)  // attempt to lock this record
				sql.query(
					"INSERT INTO openv.locks SET ?",
					lockID, (err,info) => {

					if (err)
						ctx.err = DB.errors.isLocked;

					else
						sql.query( "START TRANSACTION", err => {  // queue this transaction
							lockcb();
						});
				});	

			else  // record was never locked
				ctx.err = DB.errors.isUnlocked;

		});

	else
		ctx.err = DB.errors.noLock;
}

//================ url query expressions 

function whereify(query) {
	function proc( parms, op ) {
		function fix( key, escape ) {
			return key.parseOP( /(.*?)(\$)(.*)/, key => escape(key), (lhs,rhs,op) => {
				if (lhs) {
					var idx = rhs.split(",");
					idx.forEach( (key,n) => idx[n] = escape( n ? key : op+key) );
					return `json_extract(${escapeId(lhs)}, ${idx.join(",")} )`;
				}
				
				else
					return escapeId(rhs);
			});
		}
		
		var
			lhsEscape = escapeId,
			rhsEscape = escape;
		
		for ( var key in parms ) {
			var 
				lhs = fix( key, lhsEscape ),
				rhs = fix( parms[key], rhsEscape );

			if ( rhs.indexOf("%") >= 0 ) 
				switch (op) {
					case "=":
						ex.push( `${lhs} LIKE ${rhs}` );
						break;
					
					case "!=":
						ex.push( `${lhs} NOT LIKE ${rhs}` );
						break;
				}
			
			else
				switch ( op ) {
					case "!bin=":
						ex.push( `MATCH(${lhs}) AGAINST( '${rhs}' IN BOOLEAN MODE)` );
						break;
					case "!exp=":
						ex.push( `MATCH(${lhs}) AGAINST( '${rhs}' IN QUERY EXPANSION)` );
						break;
					case "!nlp=":
						ex.push( `MATCH(${lhs}) AGAINST( '${rhs}' IN NATURAL LANGUAGE MODE)` );
						break;
					default:
						ex.push( `${lhs} ${op} ${rhs}` );
				}
		}
	}
	
	//Log("whereify",query);
	var ex = [];
	for ( var op in query ) 
		proc( query[op], op );

	//Log(">>>where", ex);
	return ex.join(",");
}

//=============== query/fetch serialization

function serialize( qs, opts, ctx, cb ) {
/*
	sql.serialize({
		ds1: "SELECT ... ",
		ds2: "SELECT ... ", ...
		ds3: "/dataset?...", 
		ds4: "/dataset?...", ...
	}, ctx, ctx => {
		// ctx[ ds1 || ds2 || ... ] records
	});
*/
	var 
		sql = this,
		qlist = [],
		fetchRecs = function (rec, cb) {
			var
				ds = rec.ds,
				query = rec.query;
			
			if ( query.startsWith("/") ) // requesting http fetch
				if ( probeSite = DB.probeSite )
					probeSite( query, info => cb( info.parseJSON() ) );

				else  // probeSite disabled / unconfigured
					cb( null );

			else   // requesting internal db
				sql.query( 
					query, 
					[ reroute(ds) ].concat( rec.options || [] ), 
					(err, recs) => cb( err ? null : recs ) );
		};
	
	Each( qs, (ds,q)  => {
		qlist.push({
			query: q,
			ds: ds,
			options: opts
		});
	});
	
	qlist.serialize( fetchRecs, (q, recs) => {
		
		if (q) // have recs
			if (recs) 	// query ok
				if ( recs.forEach ) {  // clone returned records 
					var save = ctx[q.ds] = [];
					recs.forEach( rec => save.push( new Object(rec) ) );
				} 
		
				else  // clone returned info
					ctx[q.ds] = [ new Object(recs) ];
	
			else	// query error
				ctx[q.ds] = null;
	
		else  // at end
			cb( ctx );
	});
}

function reroute( ds , ctx ) {  //< route ds=table||db_table to a protector 
	//var 
		//routes = DB.reroute,
		//[x,db,table] = ds.match(/(.*)_(.*)/) || [ "", "app", ds ],
		//ds = db + "." + table;
	
	if ( route = DB.reroute[ds] )
		return route(ctx || {} );
	
	else
		return "app."+ds;
}

/*
function serialize( msg, query, args, cb ) {
	this.forAll( msg, query, args, (recs) => {
		recs.forEach( rec => cb(rec) );		// feed each record to callback
		cb(null);	// signal end
	});
}  */

/**
@class DB.Unit_Tests_Use_Cases
*/

switch ( process.argv[2] ) { //< unit tests
	case "?":
		Log("unit test with 'node jsdb.js [B1 || B2 || ...]'");
		break;
		
	case "B1":
	case "B2":
}
