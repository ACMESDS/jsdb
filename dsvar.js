// UNCLASSIFIED

/**
 * @class DSVAR
 * @requires cluster
 * @requires enum
 * @requires mysql
 */

var 											// nodejs
	CLUSTER = require("cluster");

var												// 3rd party bindings
	MYSQL = require("mysql");

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

/*
var 											// globals
	DEFAULT = {
		ATTRS:	{ 					// default dataset attributes
			sql: null, // sql connector
			query: "",  // sql query
			opts: null,	// ?-options to sql query
			unsafeok: true,  // allow/disallow unsafe queries
			trace: false,   // trace ?-compressed sql queries
			journal: true,	// attempt journally of updates to jou.table database
			ag: "", 		// default aggregator
			index: {select:"*"}, 	// data search and index
			client: "guest", 		// default client 
			track: false, 		// change journal tracking
			//geo: "", 	// geojson selector = key as key,key as key,...
			searchKeys: "", // fulltext match key,key,...
			doc: "", 	// table description
			jsonKeys: [] 	// json vars = key:default
		}
		// {tx: "",trace:true,unsafeok:false,journal:false,doc:"",track:0,geo:"",searchKeys:"",json:{}}
	};
*/

var
	DSVAR = module.exports = {
		
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
			
			if (opts) Copy(opts,DSVAR);
			
			Trace("CONFIG DSVAR");
			
			if (mysql = DSVAR.mysql) {
				
				mysql.pool = MYSQL.createPool(mysql.opts);

				if (sqlThread = DSVAR.thread)
					sqlThread( function (sql) {
						
						ENUM.extend(sql.constructor, [  // extend sql connector with useful methods
							getKeys,
							getFields,
							jsonKeys,
							searchKeys,
							geometryKeys,
							textKeys,
							eachRecord,
							withRecord,
							context,
							getRecord,
							getRecords
						]);

						sql.query("DELETE FROM openv.locks");
						
						cb(sql);
						
						var 
							attrs = DSVAR.attrs,
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
						});
										   
						/*
						var Attrs  = DSVAR.dsAttrs;
						sql.query(    // Defined default dataset attributes
							"SELECT * FROM openv.attrs",
							function (err,attrs) {

							if (err) 
								Log(err);

							else
								attrs.each(function (n,attr) {  // defaults
									var Attr = Attrs[attr.Dataset] = Copy({
										journal: attr.Journal,
										//tx: attr.Tx,
										flatten: attr.Flatten,
										doc: attr.Special,
										unsafeok: attr.Unsafeok,
										track: attr.Track,
										trace: attr.Trace
									}, Copy( DEFAULT.ATTRS, {} ) );
									
									//console.log([attr.Dataset, Attr]);
								});

							sql.eachTable( "app", function (tab) { // get fulltext searchable and geometry fields in tables
								var 
									Attr = Attrs[tab],
									ds = `app.${tab}`;

								if ( !Attr )
									Attr = Attrs[tab] = new Object(DEFAULT.ATTRS);

								sql.searchKeys( ds, function (keys) {
									Attr.searchKeys = keys.Escape();
								});

								sql.geometryKeys( ds, function (keys) {
									var q = "`";
									Attr.geo = keys.Escape(",", function (key) { 
										return `st_asgeojson(${q}${key}${q}) AS j${key}`; 
									});
								}); 
							});

							sql.query(   // journal all moderated datasets 
								"SELECT Dataset FROM openv.hawks GROUP BY Dataset")
							.on("result", function (mon) { 
								var Attr = Attrs[mon.Dataset] || DEFULT.ATTRS;
								Attr.journal = 1;
							});	

							sql.release();  // begonne with thee	
								
							// callback now that dsvar environment has been defined
							if (cb) cb(sql);
						});
						*/
					});
				
				else
					throw new Error("SQL thread method was not configured");
			}
			
			return DSVAR;
		},
		
		msql: null,  //< reserved for mysql connector
		
		io: {	//< reserved for socketio
			 sockets: {
				 emit: null
			 }
		},
		
		thread: function (cb) {  // callback cb(sql) with a sql connection

			function dummyConnector() {
				var
					This = this,
					err = DSVAR.errors.noDB;
				
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
				mysql = DSVAR.mysql;

			if (mysql)
				if (mysql.pool)
					mysql.pool.getConnection(function (err,sql) {
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

	};

function DATASET(sql,ats) {  // create dataset with given sql connector and attributes

	if (ats.constructor == String) ats = {table:ats};

	if (ats.table) {  // default then override attributes			
		var attrs = DSVAR.attrs[ats.table] || DSVAR.attrs.default; 

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
		Trace(DSVAR.errors.noTable+"");
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
			res( DSVAR.errors.unsafeQuery );

		else
		if ( Each(req) ) // empty so ....
			res( DSVAR.errors.nillUpdate );
		
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

				if (DSVAR.io.sockets.emit && ID && !err) 		// Notify clients of change.  
					DSVAR.io.sockets.emit( "update", {
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
			res( DSVAR.errors.unsafeQuery );
			
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
			res( DSVAR.errors.unsafeQuery );
		
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
			res( DSVAR.errors.unsafeQuery );	
		
		else
		if (me.safe || me.unsafeok) {
			me.sql.query(me.query, me.opts, function (err,info) {

				if (me.res) me.res(err || info);

				if (DSVAR.io.sockets.emit && ID && !err) 		// Notify clients of change.  
					DSVAR.io.sockets.emit( "delete", {
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
			res( DSVAR.errors.unsafeQuery );		
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

						if (DSVAR.io.sockets.emit && !err) 		// Notify clients of change.  
							DSVAR.io.sockets.emit( "insert", {
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
					me.res( DSVAR.errors.failLock );

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
							me.res( DSVAR.errors.isLocked );

						else
							sql.query( "START TRANSACTION", function (err) {  // queue this transaction
								lockcb();
							});
					});	

				else  // record was never locked
					me.res( DSVAR.errors.isUnlocked );

			});
		
		else
			me.res( DSVAR.errors.noLock );
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
									res( DSVAR.errors.noRecord );
								
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
							
							res( DSVAR.errors.noExe );
							break;

						case "select": me.rec = me.res; break;
						case "update": me.rec = me.data; break;
						case "delete": me.rec = null; break;
						case "insert": me.rec = [me.data]; break;
						case "execute": me.rec = DSVAR.errors.unsupportedQuery; break;
						
						default:
							me.rec = DSVAR.errors.invalidQuery;
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

function getFields(table, type, keys, cb) {
	this.query(`SHOW FIELDS FROM ${table} WHERE ?`,{Type:type}, function (err, recs) {
		recs.each( function (n, rec) {
			keys.push(rec.Field);
		});
		cb(keys);
	});
}

function jsonKeys(table, keys, cb) {
	this.getFields(table, "json", keys, cb);
}

function textKeys(table, keys, cb) {
	this.getFields(table, "mediumtext", keys, cb);
}

function searchKeys(table, keys, cb) {
	this.getKeys(table, "fulltext", keys, cb);
}

function geometryKeys(table, keys, cb) {
	this.getFields(table, "geometry", keys, cb);
}

function eachRecord(query, args, cb) {
	this.query(query, args).on("result", cb);
}

function withRecord(query, args, cb) {
	this.query(query, args, function (err,recs) {
		cb( err ? null : recs[0] );
	});
}

function getRecord(trace, query, args, cb) {
	var q = this.query(query, args);
	
	if (trace) Trace( q.sql + " " + trace);
	
	if (cb) q.on("result", cb);
	return q;
}

function getRecords(trace, query, args, cb) {
	var q = this.query(query, args, cb);
	
	if (trace) Trace( q.sql + " " + trace);
	
	return q;
}

function context(ctx,cb) {  // callback cb(dsctx) with a DSVAR context
	var 
		sql = this,
		dsctx = {};
	
	Each(ctx, function (dskey, dsats) {
		dsctx[dskey] = new DATASET( sql, dsats );
	});
	cb(dsctx);
}

function Trace(msg,sql) {
	ENUM.trace("V>",msg,sql);
}
