// UNCLASSIFIED

/**
 * @class dsvar
 * @requires cluster
 * requires enum
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
			function Escape(slash) {
				var q = "`";
				return  q + this.join(slash ? `${q},'${slash}',${q}` : `${q},${q}`) + q;
			}
		]
	}),
	Copy = ENUM.copy,
	Each = ENUM.each;

var 											// globals
	DEFAULT = {
		ROLE:	{trace:true,unsafeok:false,journal:false,doc:"",track:0}
	};

function Trace(msg,arg) {
	
	if (msg.constructor == String)
		console.log("S>"+msg);
	else
		console.log("S>"+msg.sql);

	if (arg) console.log(arg);
		
	return msg;
}

var
	DSVAR = module.exports = {
		
		errors: {
			unsafeQuery: new Error("unsafe queries not allowed"),
			unsupportedQuery: new Error("query not supported"),
			invalidQuery: new Error("query invalid")
		},
		
		roles: {},
		
		config: function (opts) {
			
			if (opts) Copy(opts,DSVAR);
			
			if (mysql = DSVAR.mysql) {
				mysql.pool = MYSQL.createPool(mysql.opts);

				if (thread = DSVAR.thread)
					thread( function (sql) {
						ENUM.extend(sql.constructor, [
							function context(ctx,cb) {
								var sql = this;
								var context = {};
								for (var n in ctx) context[n] = new DSVAR.DS(sql,ctx[n],{table:n});
								if (cb) cb(context);
							}
						]);

						var Roles = DSVAR.roles;
						sql.query("SELECT * FROM openv.roles", function (err,roles) {
							roles.each(function (n,role) {
								var Role = Roles[role.Table] = {
									search: [],
									journal: role.Journal,
									tx: role.Tx,
									flatten: role.Flatten,
									doc: role.Special,
									unsafeok: role.Unsafeok,
									track: role.Track,
									trace: role.Trace
								};
							});
							
							sql.query("SHOW TABLES FROM app1", function (err,tables) {
								tables.each(function (n,table) {
									var tab = table.Tables_in_app1,
										Role = Roles[tab];
									
									sql.query("SHOW KEYS FROM app1."+tab, function (err,keys) {
	
										var search = [];
										keys.each( function (n,key) {
											if (key.Index_type == "FULLTEXT")
												search.push(key.Column_name);
										});
										
										if (search.length) {
											if (!Role) Role = Roles[tab] = DEFAULT.ROLE;
											Role.search = search.Escape();
										}
									});
								});
							});		
						});
						
						sql.release();
					});	
			}
			
			return DSVAR;
		},
		
		msql: null,
		
		emit: null,
		
		thread: function (cb) {

			function nosqlConnection(err) {
		
				var sql = {
					query: function (q,args,cb) {
						//if (cb||args) (cb||args)( err );
						Trace(err+"");
						return sql;
					}, 
					on: function (ev, cb) {
						//cb( err );
						Trace(err+"");
						return sql;
					},
					sql: "", 
					release: function () {
						return sql;
					},
					createPool: function (opts) {
						return sql;
					}
				};

				return sql;
			}

			var 
				mysql = DSVAR.mysql;

			if (mysql)
				if (mysql.pool)
					mysql.pool.getConnection(function (err,sql) {
						if (err) {
							Trace( 
								err
								+ " total="	+ mysql.pool._allConnections.length 
								+ " free="	+ mysql.pool._freeConnections.length
								+ " queue="	+ mysql.pool._connectionQueue.length );

							mysql.pool.end( function (err) {
								mysql.pool = MYSQL.createPool(mysql.opts);
							});

							cb( nosqlConnection(err) );
						}
						else 
							cb( sql );
					});

				else
					cb( MYSQL.createConnection(mysql.opts) );
			else 
				cb( nosqlConnection( DSVAR.errors.noDB ) );
		},

		DS: function(sql,atts,defs) {
	
			this.sql = sql;
			this.err = null;
			this.query = "";
			this.opts = null;
			this.unsafeok = true;
			this.trace = true;
			this.journal = false;
			this.ag = "";

			var def = DSVAR.roles[atts.table] || defs || {};

			for (var n in def)
				switch (n) {
					case "select":
					case "update":
					case "delete":
					case "insert":
						this.prototype[n] = def[n];
						break;
					default:	
						this[n] = def[n];
				}

			if (atts.constructor == String) atts = {table:atts};

			for (var n in atts) this[n] = atts[n];
		}
	};

DSVAR.DS.prototype = {
	
	x: function xquery(opt,key,buf) {  // extends me.query and me.opts
		
		var me = this,
			keys = key.split(" "),
			ag = keys[1] || "least(?,1)",
			type = opt ? opt.constructor : null;
			
		if (type) 
			switch (keys[0]) {

				case "BROWSE":

					var	slash = "_", 
						where = me.where,
						nodeID = where.NodeID,
						nodes = nodeID ? nodeID.split(slash) : [],
						pivots = opt.split(",");

					me.group = (nodes.length >= pivots.length)
						? pivots.concat(["ID"])
						: pivots.slice(0,nodes.length+1);

					var name = pivots[nodes.length] || "concat('ID',ID)";
					var path = me.group.Escape(slash);

					me.query += `, cast(${name} AS char) AS name, group_concat(DISTINCT ${path}) AS NodeID`
							+ ", count(ID) AS NodeCount "
							+ ", '/tbd' AS `path`, 1 AS `read`, 1 AS `write`, 'v1' AS `group`, 1 AS `locked`";

					delete where.NodeID;
					nodes.each( function (n,node) {
						where[ pivots[n] || "ID" ] = node;
					});
					break;
					
				case "PIVOT":
					
					var where = me.where,
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
					}	
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
							me.opts.push(opt);
							break;
							
						default:
							me.unsafe = true;
					}
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
		
		var	me = this,
			table = DSVAR.roles[me.table].tx || me.table,
			ID = me.where.ID ,
			client = me.client,
			sql = me.sql,
			journal = me.journal 
				? function (cb) {
					sql.query( 						// attempt journal
						"INSERT INTO ?? SELECT *,ID AS j_ID,now() AS j_Event  FROM ?? WHERE ID=?", [
						"jou."+me.table.split(".").pop(), me.table, ID
					])						
					.on("end", cb);
				}
				: function (cb) {
					cb();
				};
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
			
		me.x(table, "UPDATE");
		me.x(req, "SET");
		me.x(me.where, "WHERE "+me.ag);
		me.x(me.order, "ORDER BY");
		
		if (me.nowhere)
			res( DSVAR.errors.unsafeQuery );

		else
		if (me.safe || me.unsafeok)
			journal( function () {
				
				sql.query(me.query, me.opts, function (err,info) {

					if (res) res( err || info );

					if (DSVAR.emit && ID && !err) 		// Notify clients of change.  
						DSVAR.emit( "update", {
							table: me.table, 
							body: req, 
							ID: ID, 
							from: client
							//flag: flags.client
						});

				});

				if (me.trace) Trace(me.query);

			});
		
		else
		if (res)
			res( DSVAR.errors.unsafeQuery );
			
	},
	
	select: function (req,res) { // select record(s) from dataset

		var	me = this,
			role = DSVAR.roles[me.table] || {},
			table = role.tx || me.table,
			client = me.client,
			sql = me.sql;
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
			
		me.x(me.index || "*", "SELECT SQL_CALC_FOUND_ROWS");
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
						if (err) return req( err, me );
						
						recs.each(function (n,rec) {
							rtn.push( Copy(rec,{}) );
						});
						
						req(rtn,me);
					});
					break;
				
				case "trace":
					Trace( sql.query(me.query, me.opts, function (err,recs) {	
						req( err || recs, me );
					}));
					break;
					
				case "all":
				default:  
					Trace( sql.query(me.query, me.opts, function (err,recs) {
						
						if (me.track && me.searching && recs)
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
					}) );
			}
		
		else
		if (res)
			res( DSVAR.errors.unsafeQuery );
		
		if (me.trace) Trace(me.query);
					
	},
	
	delete: function (req,res) {  // delete record(s) from dataset
		
		var	me = this,
			table = DSVAR.roles[me.table].tx || me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql,
			journal = me.journal 
				? function (cb) {
					sql.query( 						// attempt journal
						"INSERT INTO ?? SELECT *,ID AS j_ID,now() AS j_Event  FROM ?? WHERE ID=?", [
						"jou."+me.table.split(".").pop(), me.table, ID
					])						
					.on("end", cb);
				}
				: function (cb) {
					cb();
				};
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true;
		
		me.x(table, "DELETE FROM");
		me.x(me.where, "WHERE "+me.ag);
		
		if (me.nowhere)
			res( DSVAR.errors.unsafeQuery );	
		
		else
		if (me.safe || me.unsafeok)
			journal( function () {
			
				me.sql.query(me.query, me.opts, function (err,info) {

					if (me.res) me.res(err || info);
				
					if (DSVAR.emit && ID && !err) 		// Notify clients of change.  
						DSVAR.emit( "delete", {
							table: me.table, 
							ID: ID, 
							from: me.client
							//flag: flags.client
						});
					
				});
					
				if (me.trace) Trace(me.query);
				
			});
	
		else
		if (res)
			res( DSVAR.errors.unsafeQuery );		
	},
	
	insert: function (req,res) {  // insert record(s) into dataset
		
		function isEmpty(obj) {
			for (var n in obj) return false;
			return true;
		}
		
		var	me = this,
			table = DSVAR.roles[me.table].tx || me.table,
			ID = me.where.ID,
			client = me.client,
			sql = me.sql,
			journal = me.journal 
				? function (cb) {
					sql.query( 						// attempt journal
						"INSERT INTO ?? SELECT *,ID AS j_ID,now() AS j_Event  FROM ?? WHERE ID=?", [
						"jou."+me.table.split(".").pop(), me.table, ID
					])						
					.on("end", cb);
				}
				: function (cb) {
					cb();
				};
		
		me.opts = []; me.query = ""; me.safe = true; me.nowhere=true; 
		
		if (!req.length) req = [{}];   // force at least one insert
		
		req.each(function (n,rec) {
			sql.query(
				me.query = isEmpty(rec)
					? " INSERT INTO ?? VALUE ()"
					: " INSERT INTO ?? SET ?" ,

					[table,rec], function (err,info) {

				if (!n && res) { 					// respond only to first insert
					res( err || info );

					if (DSVAR.emit && !err) 		// Notify clients of change.  
						DSVAR.emit( "insert", {
							table: me.table, 
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

	get rec() { 
	},
	
	unlock: function (ID, cb, lockcb) {  			// unlock record 
		var me = this,
			sql = me.sql,
			lockID = {Lock:`${me.table}.${ID}`, Client:me.client};
		
		sql.query(
			"DELETE FROM openv.locks WHERE least(?)", 
			lockID, 
			function (err,info) {
				
			if (info.affectedRows) {
				cb();
				sql.query("COMMIT");  // commit queues transaction
			}
			
			else
			if (lockcb)
				sql.query(
					"INSERT INTO openv.locks SET ?",
					lockID, 
					function (err,info) {
						
					if (err)
						me.res( "record already locked by another" );

					else
						sql.query("START TRANSACTION", function (err) {  // queue this transaction
							lockcb();
						});
				});	
			
			else
				me.res( "record must be locked" );
		});
	},

	set rec(req) { 									// crud operation
		var me = this,
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
					
				default:
				
					if (me.trace) Trace(
						`${req.toUpperCase()} ${me.table} FOR ${me.client} ON ${CLUSTER.isMaster ? "MASTER" : "CORE"+CLUSTER.worker.id}`
					);
				
					switch (req) {
						case "lock.select":

							me.rec = function (recs) {

								if (recs.constructor == Error) 
									res( recs+"" );
								
								else
								if (rec = recs[0]) 
									me.unlock(rec.ID, function () {
										res( rec );
									}, function () {
										res( rec );
									});
								
								else
									res( "no record" );
								
							};
							
							break;
						
						case "lock.delete":
							
							me.unlock(ID, function () {
								me.rec = null;
							});
							break;
													
						case "lock.insert":

							me.unlock(ID, function () {
								me.rec = [me.data];
							});
							break;
							
						case "lock.update":

							me.unlock(ID, function () {
								me.rec = me.data;
							});
							break;
							
						case "lock.execute":
							
							res( "execute undefined" );
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
	
};

