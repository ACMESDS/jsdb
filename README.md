/**
@class JSDB 
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/jsdb.git) 
	[geointapps](https://git.geointapps.org/acmesds/jsdb)
	[gitlab](https://gitlab.west.nga.ic.gov/acmesds/jsdb.git)
	
# JSDB

JSDB provides a JS-agnosticated interface to any (default MySQL-Cluster) database 
as follows:
	
	JSDB.context( {ds1:{key:value, ... }, ds2:{key:value, ... }, ... }, function (ctx, sql) {
		var ds1 = ctx.ds1, ds2 = ctx.ds2, ...;
	});

or like:

	JSDB.thread( function (sql) {
		sql.context( {ds1:{key:value, ... }, ds2:{key:value, ... }, ... }, function (ctx) {
			var ds1 = ctx.ds1, ds2 = ctx.ds2, ...;
		}).end();
	});

or as a lone dataset:

	JSDB.thread( function (sql) {
		var ds = new JSDB.DS(sql, {key:value, ... });
	})

where sql is a MySQL connector, the dataset {key:value, ... } attributes are described below, and
where ds permits the following JS-agnosticated CRUD operations:

	ds.rec = { KEY:VALUE, ... }				// update matched record(s) 
	ds.rec = [ {...}, {...}, ... ]							// insert record(s)
	ds.rec = null 											// delete matched record(s)
	ds.rec = function CB(recs,me) {...}			// select matched record(s)

with callback to a CB = each | all | clone | trace method with each/all record(s) matched 
by .where, indexed by  .index, ordered by .order, grouped by .group, filtered by .having 
and limited by .limit keys defined below.

Alternatively, queries can be issued like this:

	ds.res = callback() { ... }
	ds.data = [ ... ]
	ds.rec = CRUDE

or in record-locked mode using:

	ds.rec = "lock." + CRUDE

where CRUDE = "select" | "delete" | "update" | "insert" | "execute".

Basic { key: value, ... } attributes are as follows:

	from: DB.TABLE || TABLE
	where: 	{KEY:VALUE, ... }
	tests: [ TEST, .... ]
	res: function CB(ds) {...}
	having: {KEY:VALUE, ... }
	order: KEY, ...
	sort: [ {property:KEY, direction:ORDER}, ... ]
	group: KEY, ...
	nlp: KEY
	bin: KEY
	qex: KEY
	pivot: KEY, ...
	browse: KEY, ...
	limit: VALUE
	offset: VALUE
	index: KEY, ...
	trace: true | false	
	search: PATTERN
	track: true | false

	// legacy:
	// unsafeok: 	[true] | false 		// allow potentially unsafe queries 
	// journal: true | [false] 		// enable table journalling (legacy)
	// ag: "..." 		// aggregate where/having with least(?,1), greatest(?,0), sum(?), ...

PATTERN should have a toSqlString method that renders a "KEY OP VAL" expression where
OP is typically one of [ $"<>!*/| ] to implement relational searches (eg. has, natural language, 
binary, query expansion, etc).

Non-select queries will broadcast a change to all clients if a where.ID is presented (and an emiitter
was configured), and willjournal the change when jounalling is enabled.

JSDB adds the following methods to the sql connector:

		context: establish datasets
		key getters: getKeys, getFields, jsonKeys, searchKeys, geometryKeys, textKeys
		enumerators: forFirst, forEach, forAll
		chaining: then, error, end
		misc utils: cache, flattenCatalog
		bulk insersion: beginBulk, endBulk
		job processing: selectJob, deleteJob, updateJob, insertJob, executeJob
		
and, in addition to the basic context starters:

		JSDB.thread( function cb(sql) {...} )
		JSDB.context( dsctx, function cb(ctx, sql) {...} )
		
JSDB also provides enumerators that auto-release its sql connector:

		JSDB.forFirst( trace, query, args, function cb(rec, sql) {...} )
		JSDB.forEach( trace, query, args, function cb(rec, sql) {...} )
		JSDB.forAll( trace, query, args, function cb( [rec, rec, ....] , sql) {...} )

## Databases

openv.attrs   Defines default ATTRIBUTES on startup.  
openv.hawks	 Queried for moderaters when journalling a dataset.
openv.journal	Updated with changes when journalling enabled.
openv.tracks	Updated when search tracking enabled.
openv.locks	Updated when record locks used (e.g. using forms).
app.X 	Scanned for tables that possess fulltext searchable fields.

## Use
JSDB is configured and started like this:

	var JSDB = require("../jsdb").config({
			key: value, 						// set key
			"key.key": value, 					// indexed set
			"key.key.": value,					// indexed append
			OBJECT: [ function (){}, ... ], 	// add OBJECT prototypes 
			Function: function () {} 			// add chained initializer callback
			:
			:
		}, function (err) {
		console.log( err ? "something evil happended" : "Im running");
	});

where its configuration keys follow the [ENUM copy()](https://github.com/acmesds/enum) conventions 
described in its [PRM](/shares/prm/jsdb/index.html).

Require and config JSDB:

	var JSDB = require("jsdb").config({ 
	
		dbtx: {		// table translator
			X: "DB.Y", ...
		},
		
		emit:  (crude,parms) => {  // method to bradcast changes to other socket.io clients
		}, 
		
		mysql : {	// 	database connection parms
			host: ...
			user: ...
			pass: ...
		}

	});
	
Its default DS generator and thread() method can be overridden if the default MySQL-Cluster 
support does not suffice.

Create dataset on a new sql thread

	JSDB.thread( function (sql) {
		var ds = new JSDB.DS(sql,{table:"test.x",trace:1,rec:res});
	});

Create dataset and access each record

	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,limit:[0,1],rec:function each(rec) {console.log(rec)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:['x','%ll%'],rec:function each(rec) {console.log(rec)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:['a',0,5],rec:function each(rec) {console.log(rec)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:"a<30",rec:function each(rec) {console.log(rec)}});		

Create dataset and access all records

	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:{"a<30":null,"b!=0":null,"x like '%ll%'":null,ID:5},rec:function (recs) {console.log(recs)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,order:[{property:"a",direction:"asc"}],rec:function (recs) {console.log(recs)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,index:{pivot:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});

Select ds record(s) matched by ds.where

	ds.where = [1,2];
	ds.rec = function (rec) {
		console.log(rec);
	}

Delete ds record(s) matched by ds.where

	ds.where = {ID:2}
	ds.rec = null

Update ds record(s) matched by ds.where

	ds.where = null
	ds.rec = [{a:1,b:2,ds:"hello"},{a:10,b:20,x:"there"}]
	ds.where = {ID:3}
	ds.rec = {a:100} 
	
## Installation

Clone from one of the repos.  You will typically want to redirect the following to your project

	ln -s PROJECT/totem/test.js test.js
	ln -s PROJECT/totem/maint.sh maint.sh

## License

[MIT](LICENSE)

*/
