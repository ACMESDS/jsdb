/**
@class JSDB 
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/jsdb.git) 
	[geointapps](https://git.geointapps.org/acmesds/jsdb)
	[gitlab](https://gitlab.west.nga.ic.gov/acmesds/jsdb.git)
	
# JSDB

JSDB provides a JS-agnosticated interface to any (default MySQL-Cluster) database 
as follows:
	
	JSDB.context( {ds1: ctx1, ds2: ctx2, ... }, function (ctx, sql) {
		var ds1 = ctx.ds1, ds2 = ctx.ds2, ... ;
	});

or like:

	JSDB.thread( function (sql) {
		sql.context( {ds1: ctx1, ds2: ctx2, ... }, function (ctx) {
			var ds1 = ctx.ds1, ds2 = ctx.ds2, ... ;			
		}).end();
	});

or as a lone dataset:

	JSDB.thread( function (sql) {
		sql.ctx = ctx;
	})

where sql is a MySQL connector, the dataset context ctx = {key:value, ... } are described below, 
and where the sql.ds permits the following JS-agnosticated CRUD operations:

	sql.ds = { KEY:VALUE, ... }				// update matched record(s) 
	sql.ds = [ {...}, {...}, ... ]							// insert record(s)
	sql.ds = null 											// delete matched record(s)
	sql.ds = function CB(recs,sql) {...}			// select matched record(s)
	sql.ds = "select" | "insert" | "update" | "delete" 	// locking-unlocking a record

with callback to the CB = each | all | clone | trace method with each/all record(s) matched 
by its current sql.ctx context.

Allowed context ctx ={ key: value, ... } keys include:

	crud: "select" | "insert" | "update" | "delete"
	from: "DB.TABLE" || "TABLE"
	where: 	{ KEY:VALUE, ... }
	tests: [ SQLOP, .... ]
	having: { KEY:VALUE, ... }
	set: { KEY:VALUE, ... }
	order: "KEY, ..."
	sort: [ {property:KEY, direction:ORDER}, ... ]
	group: "KEY, ..."
	nlp: "KEY, ..."
	bin: "KEY, ..."
	qex: "KEY, ..."
	pivot: "KEY, ..."
	browse: "KEY, ..."
	limit: VALUE
	offset: VALUE
	index: "KEY, ..."
	trace: true | false	
	search: "PATTERN"
	track: true | false

	// legacy:
	// unsafeok: 	[true] | false 		// allow potentially unsafe queries 
	// journal: true | [false] 		// enable table journalling (legacy)
	// ag: "..." 		// aggregate where/having with least(?,1), greatest(?,0), sum(?), ...

SQLOP should have a toSqlString method to render tests = {KEYOP:VAL, ...} (corresponding to
"KEY OP VAL" URL expressions like "x$=y&a>b" ).  These SQLOPs can implement relational 
searches (eg. has, natural language, binary, query expansion).

Non-select queries will broadcast a change to all clients if a where.ID is presented (and an 
emiitter was configured).  Update queries will journal changes when jounalling is enabled.

JSDB also provides the following methods to the sql connector:

	establish datasets> context
	key getters> getKeys, getFields, jsonKeys, searchKeys, geometryKeys, textKeys
	enumerators> forFirst, forEach, forAll
	chaining> then, error, end
	misc> cache, flattenCatalog
	bulk insersion> beginBulk, endBulk
	job processing> selectJob, deleteJob, updateJob, insertJob, executeJob
		
and several auto-release enumerators:

	JSDB.forFirst( trace, query, args, function cb(rec, sql) {...} )
	JSDB.forEach( trace, query, args, function cb(rec, sql) {...} )
	JSDB.forAll( trace, query, args, function cb( [rec, rec, ....] , sql) {...} )

## Installing

Clone from one of the repos into your PROJECT/jsdb, then:

	cd PROJECT/jsdb
	ln -s PROJECT/totem/test.js test.js 			# unit testing
	ln -s PROJECT/totem/maint.sh maint.sh 		# test startup and maint scripts
	
Dependencies:

* [ENUM basic enumerators](https://github.com/acmesds/enum)
* openv.attrs   Defines default ATTRIBUTES on startup.  
* openv.hawks	 Queried for moderaters when journalling a dataset.
* openv.journal	Updated with changes when journalling enabled.
* openv.tracks	Updated when search tracking enabled.
* openv.locks	Updated when record locks used (e.g. using forms).
* app.X 	Scanned for tables that possess fulltext searchable fields.

## Using

Each configuration follow the 
[ENUM deep copy() conventions](https://github.com/acmesds/enum):

	var DEBE = require("debe").config({
		key: value, 						// set key
		"key.key": value, 					// indexed set
		"key.key.": value					// indexed append
	}, function (err) {
		console.log( err ? "something evil is lurking" : "look mom - Im running!");
	});

where its [key:value options](/shares/prm/jsdb/index.html) override the defaults

### Create dataset on a new sql thread

	JSDB.thread( function (sql) {
		var ds = new JSDB.DS(sql,{table:"test.x",trace:1,rec:res});
	});

### Create dataset and access each record

	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,limit:[0,1],rec:function each(rec) {console.log(rec)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:['x','%ll%'],rec:function each(rec) {console.log(rec)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:['a',0,5],rec:function each(rec) {console.log(rec)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:"a<30",rec:function each(rec) {console.log(rec)}});		

### Create dataset and access all records

	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,where:{"a<30":null,"b!=0":null,"x like '%ll%'":null,ID:5},rec:function (recs) {console.log(recs)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,order:[{property:"a",direction:"asc"}],rec:function (recs) {console.log(recs)}});
	var ds = new JSDB.DS(sql,{table:"test.x",trace:1,index:{pivot:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});

### Select ds record(s) matched by ds.where

	ds.where = [1,2];
	ds.rec = function (rec) {
		console.log(rec);
	}

### Delete ds record(s) matched by ds.where

	ds.where = {ID:2}
	ds.rec = null

### Update ds record(s) matched by ds.where

	ds.where = null
	ds.rec = [{a:1,b:2,ds:"hello"},{a:10,b:20,x:"there"}]
	ds.where = {ID:3}
	ds.rec = {a:100} 
	
## License

[MIT](LICENSE)

*/
