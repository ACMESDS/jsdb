/**
@class JSDB 
	[SourceForge](https://sourceforge.net) 
	[github](https://github.com/acmesds/jsdb.git) 
	[geointapps](https://git.geointapps.org/acmesds/jsdb)
	[gitlab](https://gitlab.weat.nga.ic.gov/acmesds/jsdb.git)
	
# JSDB

JSDB provides a JS-agnosticator to a (default MySQL-Cluster) database:
	
	var JSDB = require("jsdb");
	
	JSDB.thread( function (sql) {
		sql.context( {ds1:ATTRIBUTES, ds2:ATTRIBUTES, ... }, function (ctx) {

			var ds1 = ctx.ds1, ds2 = ctx.ds2, ...;

		});
	});

where dsN are datasets, sql in a MySQL connector, and dataset ATTRIBUTES = {key:value, ... } are 
described below.  Alternatively, a lone dataset can be created thusly:

	var JSDB = require("jsdb");
	
	JSDB.thread( function (sql) {
		var ds = new JSDB.DS(sql, ATTRIBUTES);
	})

The following JS-agnosticated CRUD queries can then be performed:

	ds.rec = { FIELD:VALUE, ... }		// update matched record(s) 
	ds.rec = [ {...}, {...}, ... ]		// insert record(s)
	ds.rec = null 		// delete matched record(s)
	ds.rec = function CB(recs,me) {...}			// select matched record(s)

with callback to a response CB method when the query completes.  Alternatively,
queries can be issued like this:

	ds.res = callback() { ... }
	ds.data = [ ... ]
	ds.rec = CRUDE

or in record-locked mode using:

	ds.rec = "lock." + CRUDE

where CRUDE = "select" | "delete" | "update" | "insert" | "execute".

Dataset ATTRIBUTES = { key: value, ... } provide SQL agnostication:

	table: 	DB.TABLE || TABLE
	where: 	[ FIELD, FIELD, ... ] | { CLAUSE:null, nlp:PATTERN, bin:PATTERN, qex:PATTERN, has:PATTERN, like:PATTERN, FIELD:VALUE, FIELD:[MIN,MAX], ...} | CLAUSE
	res: 	function CB(ds) {...}
	having: [ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, CLAUSE:null, FIELD:[MIN,MAX], ...} | CLAUSE
	order: 	[ {FIELD:ORDER, ...}, {property:FIELD, direction:ORDER}, FIELD, ...] | "FIELD, ..."
	group: 	[ FIELD, ...] | "FIELD, ..."
	limit: 	[ START, COUNT ] | {start:START, count:COUNT} | "START,COUNT"
	index:	[ FIELD, ... ] | "FIELD, ... " | { has:PATTERN, nlp:PATTERN, bin:PATTERN, qex:PATTERN, browse:"FIELD,...", pivot: "FIELD,..." }

In addition, update journalling, search tracking, query broadcasting, and auto field conversion is 
supported using these ATTRIBUTES:

	unsafeok: 	[true] | false 		// allow potentially unsafe queries
	trace: [true] | false			// trace queries
	journal: true | [false] 		// enable table journalling
	search: "field,field,..." 		// define fulltext search fields
	track: true | [false] 		// enable search tracking
	ag: "..." 		// aggregate where/having with least(?,1), greatest(?,0), sum(?), ...

The select query will callback the CB=each/all/clone/trace handler with each/all record(s) matched 
by .where, indexed by  .index, ordered by .order ordering, grouped by .group, filtered by .having 
and limited by .limit ATTRIBUTES.  Select will search for PATTERN 
using its index.nlp (natural language parse), index.bin (binary mode), index.qex (query expansion), 
or group recording according to its index.browse (file navigation) or index.pivot (joint statistics).

Non-select queries will broadcast a change to all clients if a where.ID is presented (and an emiitter
was configured), and willjournal the change when jounalling is enabled.

## Usage

JSDB is configured and started like this:

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

where [its configuration keys](/shares/prm/jsdb/index.html) follow 
the [ENUM deep copy conventions](https://github.com/acmesds/enum).

Unit testing is done via:

	npm test [ ? || J1 || ... ]
	
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
	
## Installation

Clone [JSDB database agnosticator](https://github.com/acmesds/enum) into your PROJECT/jsdb folder.  
Clone [ENUM basic enumerators](https://github.com/acmesds/enum) into your PROJECT/enum folder.   

### Required MySQL Databases

openv.attrs   Defines default ATTRIBUTES on startup.  
openv.hawks	 Queried for moderaters when journalling a dataset.
openv.journal	Updated with changes when journalling enabled.
openv.tracks	Updated when search tracking enabled.
openv.locks	Updated when record locks used (e.g. using forms).
app.X 	Scanned for tables that possess fulltext searchable fields.

## Contributing

To contribute to this module, see our [issues](https://totem.west.ile.nga.ic.gov/issues.view)
and [milestones](https://totem.west.ile.nga.ic.gov/milestones.view).

## License

[MIT](LICENSE)

*/
