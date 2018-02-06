// UNCLASSIFIED

/**
 * @class JSDB
 * @requires cluster
 * @requires enum
 * @requires mysql
 */

var 											// nodejs
	CLUSTER = require("cluster");

var												// 3rd party bindings
	MYSQL = require("mysql");

var
	JSDB = module.exports = {
		
		errors: {		//< errors messages
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

		fetchers: null, 	//< defined by config 
		
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
			
			if (opts) Copy(opts,JSDB);
			
			Trace("CONFIG JSDB");
			
			if (mysql = JSDB.mysql) {
				
				mysql.pool = MYSQL.createPool(mysql.opts);

				sqlThread( function (sql) {

					ENUM.extend(sql.constructor, [  // extend sql connector with useful methods
						// key getters
						getKeys,
						getFields,
						jsonKeys,
						searchKeys,
						geometryKeys,
						textKeys,
						
						// record getters
						first,
						context,
						each,
						all,
						
						// misc
						cache,
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
						
					]);

					sql.query("DELETE FROM openv.locks");

					//cb(sql);

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
						
						if (cb) cb(sql);
						else
							sql.release();
					});
					
				});
				
			}
			
			return JSDB;
		},
		
		msql: null,  //< reserved for mysql connector
		
		io: {	//< reserved for socketio
			 sockets: {
				 emit: null
			 }
		},
		
		thread: sqlThread

	};

var 											// totem bindings
	ENUM = require("enum").extend({
		String: [
			function Escape() {
				var q = "`";
				return q + escape(this) + q;
			}
		],
				
		Array: [
			function Escape(slash,cb) {
				var q = "`";
				
				if (cb) {
					var rtn = [];
					this.each(function (n,el) {
						rtn.push( cb(el) );
					});
					return rtn.join(slash);
				}
				else						
					return  q + this.join(slash ? `${q}${slash}${q}` : `${q},${q}`) + q;
			}
		]
	}),
	Copy = ENUM.copy,
	Each = ENUM.each,
	Log = console.log;

function DATASET(sql,ats) {  // create dataset with given sql connector and attributes

	if (ats.constructor == String) ats = {table:ats};

	if (ats.table) {  // default then override attributes			
		var attrs = JSDB.attrs[ats.table] || JSDB.attrs.default; 

		for (var n in attrs)
			switch (n) {
				case "select":
				case "update":
				case "delete":
				case "insert":
					this.prototype[n] = attrs[n];
					break;
				default:	
					this[n] = attrs[n];
			}

		this.sql = sql;  	// sql connector
		this.err = null;	// default sql error response

		for (var n in ats) this[n] = ats[n];
	}

	else
		Trace(JSDB.errors.noTable+"");
}

/*
CRUD interface ds.rec = req where req is an
		Error: respond with error req
		Array: respond with statis on inserting records req into ds
		Object: respond with status on updating ds with req record then respond with status
		null: respond with status on deleting records matching ds attributes
		Function: respond on req callback with ds records matching ds attributes
		"X": dataset CRUD (ds.rec = ds.data) X=select,update,delete,create,execute 
		"lock.X": lock/unlock record for dataset CRUD
*/
DATASET.prototype = {
	
	x: function xquery(opt,key,buf) {  // query builder extends me.query and me.opts
		
		var 
			me = this,  			// target ds 
			keys = key.split(" "),  // key the the sql command
			ag = keys[1] || "least(?,1)",  // method to aggregrate 
			type = opt ? opt.constructor : null;
			
		if (type) 
			switch (keys[0]) {

				case "BROWSE":

					var	
						slash = "_", 
						where = me.where,
						nodeID = where.NodeID,
						nodes = nodeID ? nodeID.split(slash) : [],
						pivots = opt.split(",");

					me.group = (nodes.length >= pivots.length)
						? pivots.concat(["ID"])
						: pivots.slice(0,nodes.length+1);

					var name = pivots[nodes.length] || "concat('ID',ID)";
					var path = me.group.Escape(",'"+slash+"',");

					me.query += `, cast(${name} AS char) AS name, group_concat(DISTINCT ${path}) AS NodeID`
							+ ", count(ID) AS NodeCount "
							+ ", '/tbd' AS `path`, 1 AS `read`, 1 AS `write`, 'v1' AS `group`, 1 AS `locked`";

					delete where.NodeID;
					nodes.each( function (n,node) {
						where[ pivots[n] || "ID" ] = node;
					});
					break;
					
				case "PIVOT":
					
					var 
						where = me.where,
						nodeID = where.NodeID || "root";

					if (nodeID == "root") {
						me.query += ", group_concat(DISTINCT ID) AS NodeID, count(ID) AS NodeCount,false AS leaf,true AS expandable,false AS expanded";
						me.group = opt;
						delete where.NodeID;
					}
					
					else {
						me.query += `, '${nodeID}' as NodeID, 1 AS NodeCount, true AS leaf,true AS expandable,false AS expanded`;
						me.where = `instr(',${nodeID},' , concat(',' , ID , ','))`;
						me.group = null;
					}
					
					break;
					
				case "":
				case "IN":
				case "WITH":
				
					if (me.search) {
						me.query += `,MATCH(${me.search}) AGAINST('${opt}' ${key}) AS Score`;
						me.having = me.score ? "Score>"+me.score : ["Score"];
						me.searching = opt;
					}
					break;
				
				case "HAS":
				
					if (me.search) {
						me.query += `,instr(concat(${me.search}),'${opt}') AS Score`;
						me.having = me.score ? "Score>"+me.score : ["Score"];
						me.searching = opt;
					}
					break;

				case "SELECT":

					switch (type) {
						case Array:
							me.query += ` ${key} ??`;
							me.opts.push(opt);
							break;

						case String:
							if (opt == "*") 
								me.query += ` ${key} *`;
							
							else {
								me.query += ` ${key} ${opt}`;
								me.unsafe = true;
							}
							break;

						/*
						case Object:
							
							if (opt.idx) 
								me.x(opt.idx, key);
							
							else {
								me.query += ` ${key} *`;
								me.x(opt.nlp, "");
								me.x(opt.bin, "IN BINARY MODE");
								me.x(opt.qex, "WITH QUERY EXPANSION");
								me.x(opt.has,"HAS");
								me.x(opt.browse, "BROWSE");
								me.x(opt.pivot, "PIVOT");
							}
							break;
						*/
					}	
					
					me.x(me.index, "INDEX");

					/*
					if ( me.geo )  // any geometry fields are returned as geojson
						me.query += ","+me.geo;
					*/
					
					break;

				case "JOIN":
					
					switch (type) {
						case Array:
							me.query += ` ${mode} ${key} ON ?`;
							me.opts.push(opt);
							break;
							
						case String:
							me.query += ` ${mode} ${key} ON ${opt}`;
							break;

						case Object:

							var mode = opt.left ? "left" : opt.right ? "right" : "";

							me.query += ` ${mode} ${key} ? ON least(?,1)`;
							
							for (var n in opt.on) 
								buf[n] = me.table+"."+opt.on[n];
								
							me.opts.push(opt[mode]);
							me.opts.push(buf);
							break;
					}
					break;
				
				case "LIMIT":
					
					switch (type) {
						case Array:
							me.query += ` ${key} ?`;
							me.opts.push(opt);
							break;
							
						case String:
							me.query += ` ${key} ?`;
							me.opts.push(opt.split(","));
							break;

						case Object:
							me.query += ` ${key} ?`;
							me.opts.push([opt.start,opt.count]);
							break;								
					}
					break;
				
				case "WHERE":
				case "HAVING":

					me.nowhere = false;

					switch (type) {
						case Array:
							
							me.query += ` ${key} ??`;
							me.opts.push(opt);
							break;
							
						case String:
						
							me.safe = false;
							me.query += ` ${key} ${opt}`;
							break;

						case Object:
							var rels = []; 
							
							for (var n in opt) { 			// recast and remove unsafes
								var test = opt[n];
								
								if (test == null) {		// using unsafe expression query (e.g. &x<10)
									me.safe = false;
									rels.push(n);
									delete opt[n];
								}
								else 
									switch (test.constructor) {
										case Array:   // using range query e.g. x=[min,max]

											delete opt[n];
											switch (test.length) {
												case 0:
													rels.push( n.Escape() +" IS null" );
													break;

												case 2:
													rels.push(n.Escape() + " BETWEEN " + test[0] + " AND " + test[1] );
													break;

												default:
											}
											break;
											
										case Object:  // using search query e.g. x={nlp:pattern}
											
											var fld = n.Escape();
											if (pat = test.nlp) 
												rels.push( `MATCH(${fld}) AGAINST('${pat}')` );
											else
											if (pat = test.bin)
												rels.push( `MATCH(${fld}) AGAINST('${pat}') IN BINARY MODE` );
											else
											if (pat = test.qex)
												rels.push( `MATCH(${fld}) AGAINST('${pat}') WITH QUERY EXPANSION` );
											else
											if (pat = test.has)
												rels.push( `INSTR(${fld}, '${pat}')` );
											else
											if (pat = test.like)
												rels.push( `${fld} LIKE '${pat}'` );
											else
												break;
												
											delete opt[n];
											break;
											
										case String:   // otherwise using safe query e.g x=value
										default:
									}
							}
							
							for (var n in opt) { 			// aggregate where clause using least,sum,etc
								rels.push(ag);
								me.opts.push(opt);
								break;
							}
									
							rels = rels.join(" AND "); 		// aggregate remaining clauses
							if (rels)
								me.query += ` ${key} ${rels}`;
								
							else
								me.nowhere = true;
							
							break;
							
						default:
								me.unsafe = true;
					}
					break;
					
				case "ORDER":
					
					switch (type) {
						case Array:
							var by = [];
							opt.each(function (n,opt) {
								if (opt.property)
									by.push(`${opt.property} ${opt.direction}`);
								else
									for (var n in opt) 
										by.push(`${n} ${opt[n]}`);
							});
							me.query += ` ${key} ${by.join(",")}`;
							break;
							
						case String:
							me.query += ` ${key} ??`;
							me.opts.push(opt.split(","));
							break;

						case Object:
							break;
					}
					break;
					
				case "SET":
					
					switch (type) {
						/*case Array:
							me.safe = false;
							me.query += ` ${key} ??`;
							me.opts.push(opt);
							break;*/
							
						case String:
							me.safe = false;
							me.query += ` ${key} ${opt}`;
							break;

						case Object:
							
							me.query += ` ${key} ?`;
							
							/*
							for (var n in opt) {  // object subkeys are json fields
								var js = opt[n];
								if (typeof js == "object") {
									var js = JSON.stringify(js);
									me.query += `,json_merge(${n},${js})`;
									delete opt[n];
								}
							*/
							
							me.opts.push(opt);
							break;
							
						default:
							me.unsafe = true;
					}
					break;
					
				case "INDEX":
					//console.log(opt);
					me.x(opt.nlp, "");
					me.x(opt.bin, "IN BINARY MODE");
					me.x(opt.qex, "WITH QUERY EXPANSION");
					me.x(opt.has,"HAS");
					me.x(opt.browse, "BROWSE");
					me.x(opt.pivot, "PIVOT");
					break;
					
				default:
					
					switch (type) {
						case Array:
							me.query += ` ${key} ??`;
							me.opts.push(opt);
							break;
							
						case String:
							me.query += ` ${key} ??`;
							me.opts.push(opt.split(","));
							break;

						case Object:
							me.query += ` ${key} ?`;
							me.opts.push(opt);
							break;
					}
			}

	},
	
	update: function (req,res) { // update record(s) in dataset

		function hawkChange(log) {  // journal changes 
			sql.query("SELECT * FROM openv.hawks WHERE least(?,Power)", log)
			.on("result", function (hawk) {
//console.log(hawk);
				sql.query(
					"INSERT INTO openv.journal SET ? ON DUPLICATE KEY UPDATE Updates=Updates+1",
					Copy({
						Hawk: hawk.Hawk,  	// moderator
						Power: hawk.Power, 	// moderator's level
						Updates: 1 					// init number of updates made
					}, log)
				);
			});
		}
		
		var	
			me = this,
			table = me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
			
		me.x(table, "UPDATE");
		me.x(req, "SET");
		me.x(me.where, "WHERE "+me.ag);
		me.x(me.order, "ORDER BY");
		
		if (me.nowhere)
			res( JSDB.errors.unsafeQuery );

		else
		if ( Each(req) ) // empty so ....
			res( JSDB.errors.nillUpdate );
		
		else
		if (me.safe || me.unsafeok) {
			/*
			//uncomment to disable journalling
			me.journal = false;
			*/
			
			if (me.journal) {  // attempt change journal if enabled
				hawkChange({Dataset:me.table, Field:""});  // journal entry for the record itself
				/*
				// uncomment to enable
				for (var key in req) { 		// journal entry for each record key being changed
					hawk({Dataset:me.table, Field:key});
					hawk({Dataset:"", Field:key});
				}
				*/
			}
			
			sql.query(me.query, me.opts, function (err,info) {

				if (res) res( err || info );

				if (JSDB.io.sockets.emit && ID && !err) 		// Notify clients of change.  
					JSDB.io.sockets.emit( "update", {
						path: "/"+me.table+".db", 
						body: req, 
						ID: ID, 
						from: client
						//flag: flags.client
					});

			});

			if (me.trace) Trace(me.query);
		}
		
		else
		if (res)
			res( JSDB.errors.unsafeQuery );
			
	},
	
	select: function (req,res) { // select record(s) from dataset
		
		var	
			me = this,
			table = me.table,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true; 
		
		me.x(me.index.select, "SELECT SQL_CALC_FOUND_ROWS");
		me.x(table, "FROM");
		me.x(me.join, "JOIN", {});
		me.x(me.where, "WHERE "+me.ag);
		me.x(me.having, "HAVING "+me.ag);
		me.x(me.order, "ORDER BY");
		me.x(me.group, "GROUP BY");
		me.x(me.limit, "LIMIT");

		if (me.safe || me.unsafeok)
			switch (req.name) {
				case "each": 
					sql.query(me.query, me.opts)
					.on("error", function (err) {
						req(err,me);
					})						
					.on("result", function (rec) {
						req(rec,me);
					});
					break;
				
				case "clone": 
					var rtn = [];
					sql.query(me.query, me.opts, function (err,recs) {	
						if (err) 
							return req( err, me );
						
						recs.each(function (n,rec) {
							rtn.push( new Object( rec ) );
						});
						
						req(rtn,me);
					});
					break;
				
				case "trace":
					Trace(me.query);
					sql.query(me.query, me.opts, function (err,recs) {	
						req( err || recs, me );
					});
					break;
					
				case "all":
				default:  
					sql.query(me.query, me.opts, function (err,recs) {
						
						if (me.track && me.searching && recs)  // track searches if tracking
							sql.query(
								"INSERT INTO openv.tracks SET ? ON DUPLICATE KEY UPDATE Searched=Searched+1,Returned=Returned+?", [
									{	Client: client,
									 	Searching: me.searching,
									 	Searched: 0,
									 	Within: me.table,
									 	Returned: recs.length
									}, recs.length
								]);
						
						req( err || recs, me );
					});
			}
		
		else
		if (res)
			res( JSDB.errors.unsafeQuery );
		
		if (me.trace) Trace(me.query);
					
	},
	
	delete: function (req,res) {  // delete record(s) from dataset
		
		var	
			me = this,
			table = me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
		
		me.x(table, "DELETE FROM");
		me.x(me.where, "WHERE "+me.ag);
		
		if (me.nowhere)
			res( JSDB.errors.unsafeQuery );	
		
		else
		if (me.safe || me.unsafeok) {
			me.sql.query(me.query, me.opts, function (err,info) {

				if (me.res) me.res(err || info);

				if (JSDB.io.sockets.emit && ID && !err) 		// Notify clients of change.  
					JSDB.io.sockets.emit( "delete", {
						path: "/"+me.table+".db", 
						ID: ID, 
						from: me.client
						//flag: flags.client
					});

			});

			if (me.trace) Trace(me.query);
		}
	
		else
		if (res)
			res( JSDB.errors.unsafeQuery );		
	},
	
	insert: function (req,res) {  // insert record(s) into dataset
		
		var	
			me = this,
			table = me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true; 
		
		if (!req.length) req = [{}];   // force at least one insert attempt
		
		req.each(function (n,rec) { // insert each record
			
			sql.query(
				me.query = Each(rec)  // trap empty records
					? "INSERT INTO ?? VALUE ()"  
					:  "INSERT INTO ?? SET ?" ,

				[table,rec], 
				
				function (err,info) {

					if (!n && res) { 					// respond only to first insert
						res( err || info );

						if (JSDB.io.sockets.emit && !err) 		// Notify clients of change.  
							JSDB.io.sockets.emit( "insert", {
								path: "/"+me.table+".db", 
								body: rec, 
								ID: info.insertId, 
								from: client
								//flag: flags.client
							});
					}			

			});

			if (me.trace) Trace(me.query);
		});

	},

	get rec() {   // reserved
	},
	
	unlock: function (ID, cb, lockcb) {  			// unlock record 
		var 
			me = this,
			sql = me.sql,
			lockID = {Lock:`${me.table}.${ID}`, Client:me.client};
		
		if (ID)
			sql.query(  // attempt to unlock a locked record
				"DELETE FROM openv.locks WHERE least(?)", 
				lockID, 
				function (err,info) {

				if (err)
					me.res( JSDB.errors.failLock );

				else
				if (info.affectedRows) {  // unlocked so commit queued queries
					cb();
					sql.query("COMMIT");  
				}

				else 
				if (lockcb)  // attempt to lock this record
					sql.query(
						"INSERT INTO openv.locks SET ?",
						lockID, 
						function (err,info) {

						if (err)
							me.res( JSDB.errors.isLocked );

						else
							sql.query( "START TRANSACTION", function (err) {  // queue this transaction
								lockcb();
							});
					});	

				else  // record was never locked
					me.res( JSDB.errors.isUnlocked );

			});
		
		else
			me.res( JSDB.errors.noLock );
	},

	set rec(req) { 									// crud interface
		var 
			me = this, 
			res = me.res;
		
		if (req) 
			switch (req.constructor) {
				case Error:
				
					if (res) res(req);
					break;
					
				case Array: 

					me.insert(req,res);
					break;
					
				case Object:

					me.update(req,res);
					break;
					
				case Function:
					me.select(req,res);
					break;
					
				case String:
				
					if (me.trace) Trace(
						`${req.toUpperCase()} ${me.table} FOR ${me.client}`
					);
				
					switch (req) {
						case "lock.select":

							me.rec = function (recs) {  // get records

								if (recs.constructor == Error) 
									res( recs );
								
								else
								if (rec = recs[0]) 
									me.unlock(rec.ID, function () {  // unlocked
										res( rec );
									}, function () {  // locked
										res( rec );
									});
								
								else
									res( JSDB.errors.noRecord );
								
							};
							break;
						
						case "lock.delete":
							
							me.unlock(me.where.ID, function () {  // delete if unlocked
								me.rec = null;
							});
							break;
													
						case "lock.insert":

							me.unlock(me.where.ID, function () { // insert if unlocked
								me.rec = [me.data];
							});
							break;
							
						case "lock.update":

							me.unlock(me.where.ID, function () {  // update if unlocked
								me.rec = me.data;
							});
							break;
							
						case "lock.execute":
							
							res( JSDB.errors.noExe );
							break;

						case "select": me.rec = me.res; break;
						case "update": me.rec = me.data; break;
						case "delete": me.rec = null; break;
						case "insert": me.rec = [me.data]; break;
						case "execute": me.rec = JSDB.errors.unsupportedQuery; break;
						
						default:
							me.rec = JSDB.errors.invalidQuery;
					}

			}
		
		else 
			me.delete(req,res);
		
		return this;
	}
	
}

function getKeys(table, type, keys, cb) {
	this.query(`SHOW KEYS FROM ${table} WHERE ?`,{Index_type:type}, function (err, recs) {
		recs.each( function (n,rec) {
			keys.push(rec.Column_name);
		});
		cb(keys);
	});
}

function getFields(table, where, keys, cb) {
	this.query(`SHOW FULL FIELDS FROM ${table} WHERE least(?,1)`,where, function (err, recs) {
		recs.each( function (n, rec) {
			keys.push(rec.Field);
		});
		cb(keys);
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

function first(trace, query, args, cb) {  // callback cb(rec) or cb(null) if error
	var q = query 
		? this.query( smartTokens(query,args), args, function (err,recs) {
				cb( err ? null : recs[0] );
			})
	
		: { sql: "IGNORE", on: function (){} };
	
	if (trace) Trace( trace + " " + q.sql);	
	return q;
}

function each(trace, query, args, cb) { // callback cb(rec) with each rec
	var q = query 
		? this.query( smartTokens(query,args), args).on("result", cb)
	
		: { sql: "IGNORE", on: function (){} };
	
	if (trace) Trace( trace + " " + q.sql);	
	return q;
}

function all(trace, query, args, cb) { // callback cb(recs) if no error
	var q = query
		? this.query( smartTokens(query,args), args, function (err,recs) {
				if (!err) if(cb) cb( recs );
			})
	
		: { sql: "IGNORE", on: function (){} };
	
	if (trace) Trace( trace + " " + q.sql);	
	return q;
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
				if (err) {
					Log({
						sqlpool: err,
						total: mysql.pool._allConnections.length ,
						free: mysql.pool._freeConnections.length,
						queue: mysql.pool._connectionQueue.length
					});

					/*mysql.pool.end( function (err) {
						mysql.pool = MYSQL.createPool(mysql.opts);
					}); */

					cb( new dummyConnector(err) );
				}
				else 
					cb( sql );
			});
	
		else
			cb( MYSQL.createConnection(mysql.opts) || new dummyConnector( ) );

	else 
		cb( new dummyConnector( ) ); 
}

/*
Implements generic cache.  Looks for cache given opts.key and, if found, returns cached results on cb(results);
otherwse, if not found, returns results via opts.make(fetchers, opts.parms, cb).  If cacheing fails, then opts.default 
is returned.  The returned results will always contain a results.ID for its cached ID.  If a opts.default is not provided,
then the cb callback in not made.
*/

function cache( opts, cb ) {
	var sql = this;
	
	if ( opts.key )
		sql.first( 
			"", 
			"SELECT Results FROM app.cache WHERE least(?,1) LIMIT 1", 
			[ opts.key ], function (rec) {

			if (rec) 
				try {
					cb( JSON.parse(rec.Results) );
				}
				catch (err) {
					if ( opts.default )
						cb( Copy(opts.default, {ID: null} ) );
				}

			else
			if ( opts.make && opts.parms ) 
				opts.make( JSDB.fetchers, opts.parms, function (res) {

					if (res) 
						sql.query( 
							"INSERT INTO app.cache SET Added=now(), Results=?, ?", 
							[ JSON.stringify(res || opts.default), opts.key ], 
							function (err, info) {
								if (err) {
									if ( opts.default ) 
										cb( Copy(opts.default, {ID: null}) );
								}

								else 
									cb( Copy(res, {ID: info.insertId}) );
						});

					else 
					if ( opts.default )
						cb( Copy(opts.default, {ID: null}) );
				});

			else
			if ( opts.default )
				cb( Copy(opts.default, {ID: null}) );
		});
	
	else
	if ( opts.default )
		cb( Copy({ID: null}, opts.default) );
	
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
	
/*
@method selectJob
@param {Object} req job query
@param {Function} cb callback(rec) when job departs
*
* Callsback cb(rec) for each queuing rec matching the where clause.
* >>> Not used but needs work 
 */
function selectJob(where, cb) { 

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

/*
@method updateJob
@param {Object} req job query
@param {Function} cb callback(sql,job) when job departs
*
* Adjust priority of jobs matching sql-where clause and route to callback cb(req) when updated.
* >>> Not used but needs work 
*/
function updateJob(req, cb) { 
	
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
		
/*
@method deleteJob
@param {Object} req job query
@param {Function} cb callback(sql,job) when job departs
* >>> Not used but needs work
*/
function deleteJob(req, cb) { 
	
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
billing information.  When using insertJob within an async loop, the caller should pass a cloned copy
of the job.
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
				regulate( job , function (job) { // provide callback when job departs
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

/**
 @method flattenCatalog
 Flatten entire database for searching the catalog
 * */
function flattenCatalog(flags, catalog, limits, cb) {
	
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

			Trace( sql.query( query, args,  function (err,recs) {
				
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
			}) );	
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

function smartTokens(q, opts) {
	var 
		optn = 0,
		args = (opts.constructor == Array) ? null : opts;
	
	return q.replace(/\?/g, function (mat,idx) {
		var 
			tests = ["?"] , 
			quote = "`",
			opt = args || opts[optn++];
		
		if ( opt.constructor == Object ) {
			Each(opt, function (key,val) {
				switch ( op = key.substr(-1) ) {
					case "<":
					case ">":
					case "!":
						tests.push( key + "=" + val );
						delete opt[key];
						break;
					default:
						if ( !val ) {
							tests.push( key );
							delete opt[key];
						}
				}
			});
			
			Log("smart", tests, q);
			return tests.join(",");
		}
		
		else
			return "?";
	});
}
	
function Trace(msg,sql) {
	ENUM.trace("V>",msg,sql);
}
