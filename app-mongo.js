/*
Commands:
run mongod in one shell, or mongod --fork as a bg process

mongo.exe
db.test.save({a:1})
db.test.find()
db.stats()
*/
/*
Notes

A simple insert of this document failed because the process ran out of memory. 
Just to know the results, i decreased the limit of 10^6 entries to 10^4
So I inserted 100000 entries and it took 4138 ms, so 0.04138 per entry (small entry of 266 characters). 


Now to test it out, I made an insert statement for biger entries (10x in size, 2600 chars)   
Results: 100000 big entries : 8628. So a 10x increase in entry size leads to a 2x slow down 

Then just to try it out I inserted 100000 entries that are 100x the size of the small entry.
It took 12140 ms. So time grows as LogN

GRAPH: https://www.wolframalpha.com/input/?i=(260,16),+(2600,20),+(26000,25)

Chars | Time
---------------
260   | 16
2600  | 20
26000 | 25 

*/
/*
0 - find
1 - delete
2..4 - insert [small,medium,big]
6..7 - read all big [small,medium,big]
*/ 

//lets require/import the mongodb native drivers.
var mongodb = require('mongodb');
var async = require('async');

//We need to work with "MongoClient" interface in order to connect to a mongodb server.
var MongoClient = mongodb.MongoClient;

// Connection URL. This is where your mongodb server is running.
var url = 'mongodb://localhost:27017/stress_test';

TestDB = {

	insert : function(db, limit, type, text, caller){

		console.log("Starting insert ["+type+"]");

		var arr = [];
	    for (var i = 0; i < limit; i++) {  
	    	arr.push({
    			"type": "type",
			    "tex": text,
			    "borough": "Manhattan",
			    "cuisine": "Italian"
			});
	    }

        var start = Date.now();   
        var calls = [];  
		var counter = 0;
 		
 		for(var elem in arr){
 
			calls.push(function(callback) { 
 
				elem = arr[counter]; 
	 			counter++;

				db.collection('restaurants').insert(elem, function(err, result) {

					if(err || result===null || result.ops===undefined || result.ops.length == 0){
						
						console.log(err);
						callback(); 
						return;
					}

					result = result.ops[0]; 
					db.collection('addresses').insert({ 
						"restaurant": result._id,
				        "street": "2 Avenue",
				        "zipcode": "10075",
				        "building": "1480",
				        "coord": [-73.9557413, 40.7720266]  
					}, function(err, result) {
						  
						callback(); 
					});
				});
			}); 
        }

		async.parallel(calls, function(err, result) {

		    if (err){  
		        console.log(err); 
		    }else{

		        var end = Date.now();
	            if(result){
	                console.log("Time to insert " + result.length + " "+type+" entries : " + (end - start));
	            	
	            	if(caller!==undefined && caller!==null)
	            		caller();
	            }
            } 
		}); 
	},
	remove : function(db, callback){

		console.log("Starting delete ");
		db.collection('restaurants').deleteMany({},function(err, results) {
	        console.log("Removed all restaurants"); 
	        db.collection('addresses').deleteMany({},function(err, results) {
		        console.log("Removed all addresses"); 
		        callback();
		    });
	    });  
	},
	find : function(db, type, caller){

		console.log("Starting find ["+type+"]"); 

		var start = Date.now();  

        var calls = []; 
 
		calls.push(function(callback) {  
			db.collection('addresses').find(  
				{},//find all
				{ restaurant: 1 },//return only this field
		        {}) //options  [limit:10]
			.forEach(function(item) { 

				if(item!=null){
							
		 			//this is a one to one relationship to this forEach runs once only 
					db.collection('restaurants').findOne( 
						{ _id: item.restaurant, type: type },//query
						{ type: 1 })
					.then(function(one){  
						callback();
					});
				}
			});
		});

		async.parallel(calls, function(err, result) {

		    if (err){  
		        console.log(err); 
		    }else{

		        var end = Date.now();
	            if(result){
	                console.log("Time to find "+type+" entries : " + (end - start));
	            	
	            	if(caller!==undefined && caller!==null)
	            		caller();
	            }
            }
            
		});  
	}, 
	join : function(db, callback){
 
		console.log("Starting join");
 
		var start = Date.now(); 
		db.collection('addresses')
			.aggregate([   
		        { $project : { restaurant: 1} },
		        { $limit : 10000 },  
				{ $lookup: { 
					from: 'restaurants',
					localField: "restaurant",
					foreignField: "_id", 
					as : "item"
				}},
			]).toArray(function(err, items) {
				
				if (err){  
			        console.log(err); 
			    }else{

			        var end = Date.now(); 
		            if(items){
		                console.log("Time to join "+items.length+" entries : " + (end - start));

		            	if(callback!==undefined && callback!==null)
			            	return callback(items); 
		            } 
	            }  
			});
	}
}


MongoClient.connect(url, function(err, db) {

	var operation = 0;
	if(operation==0){//insert 

		TestDB.remove(db,function(){

			var limit;

			limit = 100000;
			/* 
			Time to insert 10000 small entries : 4719 
			Time to insert 10000 medium entries : 3892 
			Time to insert 10000 big entries : 4974 
			*/ 
 			
			TestDB.insert(db, limit, "small",{
				"tex" : "small text"
			}, function() {
 
				TestDB.insert(db, limit, "medium",{
					"tex": "MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.",    				        
				}, function() {
				    //TestDB.insert(db, limit, "big", {
				    //	"tex": "MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.MongoDB’s aggregation framework ouping and sorting documents by specific field or fields as well as tools for aggregating the contents of arrays, including arrays of documents. In addition, pipeline stages can use operators for tasks such as calculating the average or concatenating a string. The pipeline provides efficient data aggregation using native operations within MongoDB, and is the preferred method for data aggregation in MongoDB. The aggregation pipeline can operate on a sharded collection. The aggregation pipeline can use indexes to improve its performance during some of its stages. In additwo phases: a map stage that processes each document and emits one or more objects for each input document, and reduce phase that combines the output of the map operation. Optionally, map-reduce can have a finalize stage to make final modifications to the result. Like other aggregation operations, map-reduce can specify a query condition to select the input documents as well as sort and limit the results. Map-reduce uses custom JavaScript functions to perform the map and reduce operations, as well as the optional finalize operation. While the custom JavaScript provide great flexibility compared to the aggregation pipeline, in general, map-reduce is less efficient and more complex than the aggregation pipeline. Map-reduce can operate on a sharded collection. Map reduce operations can also output to a sharded collection. See Aggregation Pipeline and Sharded Collections and Map-Reduce and Sharded Collections for details.",
				    //});
				});
			});
		});

	}else if(operation==1){//simple lookup

		/* 
		Time to find small entries : 36  
		Time to find medium entries : 74 
		Time to find big entries : 389 
		*/

		TestDB.find(db, "small", function(){
			TestDB.find(db, "medium", function(){
				TestDB.find(db, "big");
			});
		});

	}else if(operation==2){

		/*
		Time to join 10000 entries : 706
		*/ 

		TestDB.join(db, function(){

			console.log("done with join");
		}); 
	}
}); 