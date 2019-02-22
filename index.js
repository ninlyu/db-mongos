const mongodb = require('mongodb').MongoClient;

class MongoConn
{
	constructor(cfg)
	{
		this.config_ = cfg;
	}

	connection(callback)
	{
		// use method: db.close() to disconnection.
		mongodb.connect(this.config_.url+'/'+this.config_.dbname, {useNewUrlParser:true}, (err, db) => {
			callback(db);
		});
	}

	dropTable(db, tblname, callback)
	{
		db.db().collection(tblname).drop((err, res) => {
			callback(db);
		})
	}

	count(db, tblname, callback)
	{
		db.db().collection(tblname).find({}).count((err, res) => {
			callback(db, res);
		})
	}

	insertOne(db, tblname, model, callback)
	{
		db.db().collection(tblname).insertOne(model, (err, res) => {
			callback(db);
		})
	}

	insertAll(db, tblname, models, callback)
	{
		db.db().collection(tblname).insertMany(models, (err, res) => {
			callback(db);
		})
	}

	deleteOne(db, tblname, where, callback)
	{
		db.db().collection(tblname).deleteOne(where, (err, res) => {
			callback(db);
		})
	}

	deleteAll(db, tblname, where, callback)
	{
		db.db().collection(tblname).deleteAll(where, (err, res) => {
			callback(db);
		})		
	}

	selectOne(db, tblname, fields, where, callback, refield=false)
	{
	    var val = 1;
	    var opts = { projection:{_id:0} };
	    if (refield) val = 0;
	    for (let i = 0; i < fields.length; i++)
	        opts.projection[fields[i]] = val;

		db.db().collection(tblname).findOne(where, opts, (err, res) => {
			callback(db, res);
		})
	}

	selectAll(db, tblname, fields, where, callback, refield=false)
	{
		var val = 1;
	    var params = {_id:0};
	    if (refield) val = 0;
	    for (let i = 0; i < fields.length; i++)
	        params[fields[i]] = val;

		db.db().collection(tblname).find(where).project(params).toArray((err, res) => {
			callback(db, res);
		})
	}

	updateOne(db, tblname, sets, where, callback, upsert=false)
	{
		var pattern = upsert ? {'upsert':true} : {};

		db.db().collection(tblname).updateOne(where, sets, pattern, (err, res) => {
			callback(db, res);
		})
	}

	updateAll(db, tblname, sets, where, callback, upsert=false)
	{
		var pattern = upsert ? {'upsert':true} : {};

		db.db().collection(tblname).updateMany(where, sets, pattern, (err, res) => {
			callback(db, res);
		})
	}

	union(db, tblname, fields, where, lookups, callback)
	{
	    var params = [];
	    params.push({$match:where});
	    fields._id = 0;
	    params.push({$project:fields});

	    for (let i = 0; i < lookups.length; i++)
	        params.push(lookups[i]);

		db.db().collection(tblname).aggregate(params, {cursor:{batchSize:100}}, (err, cursor) => {
			cursor.get((err, res) => {
				callback(db, res);
			})
		})
	}
}

module.exports = MongoConn;