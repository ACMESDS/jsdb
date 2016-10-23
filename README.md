/**
@class dsvar [![Forked from SourceForge](https://sourceforge.net)]
# DSVAR

The DSVAR NodeJS modules is a database agnosticator that provides a normalized JS dataset interface to a (default 
MySQL-Cluster) database using:
	
	sql.context( {ds1:ATTRIBUTES, ds2:ATTRIBUTES, ... }, function (ctx) {

		var ds1 = ctx.ds1, ds2 = ctx.ds2, ...;

	});

where dsX are datasets and where sql in a MySQL connector.  Or, a lone dataset ds can be created thusly:

	var DSVAR = require("dsvar"),
		  ds = new DSVAR.DS(sql, ATTRIBUTES);

where ATTRIBUTES = {key:value, ... } are described below.  In this way, dataset queries can be 
performed in a db-agnostic way using:

	ds.rec = { FIELD:VALUE, ... }				// update matched record(s) 
	ds.rec = [ {...}, {...}, ... ]						// insert record(s)
	ds.rec = null 										// delete matched record(s)
	ds.rec = function CB(recs,me) {...}		// select matched record(s)

with callback to its non-null response .res method when the query completes.  Any CRUDE 

		"select" | "delete" | "update" | "insert" | "execute" 

query can also be performed using:

	ds.res = callback() { ... }
	ds.data = [ ... ]
	ds.rec = CRUDE

or in record-locked mode (as reflected to the **openv.lock** table) using:

	ds.rec = "lock." + CRUDE

Dataset ATTRIBUTES = { key: value, ... } include:

	table: 	DB.TABLE || TABLE
	where: 	[ FIELD, FIELD, ... ] | { CLAUSE:null, nlp:PATTERN, bin:PATTERN, qex:PATTERN, has:PATTERN, like:PATTERN, FIELD:VALUE, FIELD:[MIN,MAX], ...} | CLAUSE
	res: 	function (ds) {...}

	having: [ FIELD, VALUE ] | [ FIELD, MIN, MAX ] | {FIELD:VALUE, CLAUSE:null, FIELD:[MIN,MAX], ...} | CLAUSE
	order: 	[ {FIELD:ORDER, ...}, {property:FIELD, direction:ORDER}, FIELD, ...] | "FIELD, ..."
	group: 	[ FIELD, ...] | "FIELD, ..."
	limit: 	[ START, COUNT ] | {start:START, count:COUNT} | "START,COUNT"
	index:	[ FIELD, ... ] | "FIELD, ... " | { has:PATTERN, nlp:PATTERN, bin:PATTERN, qex:PATTERN, browse:"FIELD,...", pivot: "FIELD,..." }

as well as derived ATTRIBUTES (derived from the **openv.roles** table at startup):

	unsafeok: 	[true] | false 		allow potentially unsafe queries
	trace: [true] | false				trace queries
	journal: true | [false] 			enable table journalling
	search: "field,field,..." 			define fulltext search fields
	track: true | [false] 				enable search tracking
	ag: "..." 								aggregate where/having with least(?,1), greatest(?,0), sum(?), ...
	tx: "db.table" 						translate table

Attributes will null keys are always ignored.

The select query will callback the CB=each/all/clone/trace handler with each/all record(s) matched 
by .where, indexed by  .index, ordered by .order ordering, grouped by .group, filtered by .having 
and limited by .limit ATTRIBUTES.  Select will search for PATTERN 
using its index.nlp (natural language parse), index.bin (binary mode), index.qex (query expansion), 
or group recording according to its index.browse (file navigation) or index.pivot (joint statistics).

Non-select queries will broadcast a change to all clients if a where.ID is presented (and an emiitter
was configured), and willjournal the change when jounalling is enabled.

## Examples

Require and config DSVAR:

	var DSVAR = require("dsvar").config({ 
	
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
	
The .DS and .thread options can be overridden if the default MySQL-Cluster support does not suffice.

Create dataset on a new sql thread

	DSVAR.thread( function (sql) {
	
		var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,rec:res});
		
	});

Create dataset and access each record

	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,limit:[0,1],rec:function each(rec) {console.log(rec)}});
	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,where:['x','%ll%'],rec:function each(rec) {console.log(rec)}});
	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,where:['a',0,5],rec:function each(rec) {console.log(rec)}});
	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,where:"a<30",rec:function each(rec) {console.log(rec)}});		

Create dataset and access all records

	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,where:{"a<30":null,"b!=0":null,"x like '%ll%'":null,ID:5},rec:function (recs) {console.log(recs)}});
	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,order:[{property:"a",direction:"asc"}],rec:function (recs) {console.log(recs)}});
	var ds = new DSVAR.DS(sql,{table:"test.x",trace:1,index:{pivot:"root"},group:"a,b",rec:function (recs) {console.log(recs)}});

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

Download the latest version with

	git clone https://git.geointapps.org/acmesds/dsvar

Typically, you will want to redirect the following to your project/master

	ln -s ../master/test.js test.js
	ln -s ../master/maint.sh maint.sh

## License

[MIT](LICENSE)

*/