/*
Note:

For NoSQL, there was only the `restaurants` collection. Here
I have the restaurants and the associated with them addresses in 2 tables  
*/

/*
DB SCHEMA

-- phpMyAdmin SQL Dump
-- version 4.5.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Jun 15, 2016 at 12:40 AM
-- Server version: 10.1.13-MariaDB
-- PHP Version: 7.0.5

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

 
--
-- Database: `stress_test`
--
CREATE DATABASE IF NOT EXISTS `stress_test` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `stress_test`;

-- --------------------------------------------------------

--
-- Table structure for table `addresses`
--

DROP TABLE IF EXISTS `addresses`;
CREATE TABLE `addresses` (
  `id` int(11) NOT NULL,
  `street` varchar(255) NOT NULL,
  `zipcode` int(8) NOT NULL,
  `building` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `addresses`
--

INSERT INTO `addresses` (`id`, `street`, `zipcode`, `building`) VALUES
(1, '2 Avenue', 10075, '1480');

-- --------------------------------------------------------

--
-- Table structure for table `restaurants`
--

DROP TABLE IF EXISTS `restaurants`;
CREATE TABLE `restaurants` (
  `id` int(10) NOT NULL,
  `type` varchar(255) NOT NULL,
  `text` text NOT NULL,
  `borough` varchar(255) NOT NULL,
  `cuisine` varchar(255) NOT NULL,
  `address` int(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `addresses`
--
ALTER TABLE `addresses`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `restaurants`
--
ALTER TABLE `restaurants`
  ADD PRIMARY KEY (`id`),
  ADD KEY `address` (`address`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `addresses`
--
ALTER TABLE `addresses`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;
--
-- AUTO_INCREMENT for table `restaurants`
--
ALTER TABLE `restaurants`
  MODIFY `id` int(10) NOT NULL AUTO_INCREMENT;

*/

var mysql = require("mysql");
var async = require('async');

// First you need to create a connection to the db
var db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "",
  database : 'stress_test'
}); 

/*
Format SQL query
*/
function _q(str){ 
	str = str.join(""); 
	str = str.trim(); 
	str = str.replace(/\r?\n|\r/g, ""); 
	return str;
};

TestDB = {
	
	insert : function(db, limit, type, data, caller){

		console.log("Starting insert ["+type+"]");
 
        var start = Date.now();  

        var calls = [];

		var address 	= "INSERT INTO addresses 			 	" +
			"(street,zipcode,building)   			" +
			"VALUES ('" +data.address.street+"',    "+
			"'" +data.address.zipcode+"', 	"+
			"'" +data.address.building+"');	";

		var address2_front= "INSERT INTO restaurants 			    "+
			"(type,text,address,borough,cuisine)   	"+
			"VALUES ('"+data.restaurant.type+"',    "+
			"'"+data.restaurant.text+"','$ADDRESSID', "+
			"'"+data.restaurant.borough+"', "+
			"'"+data.restaurant.cuisine+"');";

	    for (var i = 0; i < limit; i++) {

			calls.push(function(callback) {  

				 db.query(address,function(err,result){
 					if(err) {
						console.log(err);
 						return;
 					}

 					//link 2 table entries
					 var restaurant = address2_front.replace("$ADDRESSID",result.insertId);

 					db.query(restaurant,function(err,result){  
						if(err) {
							console.log(err);
	 						return;
	 					}
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

	 	var str = "TRUNCATE TABLE addresses"
		db.query(str,function(err,result){ 
			if(err) {
				console.log(err);
				return; 
			}

			str = "TRUNCATE TABLE restaurants"
			db.query(str,function(err,result){ 
				if(err) {
					console.log(err);
					return; 
				}
				console.log("Removed all"); 
				callback();
			});
		}); 

	},
	find : function(db, type, callback){

		console.log("Starting find ["+type+"]"); 

		var start = Date.now();  
		var str = "SELECT type FROM restaurants WHERE type='"+type+"'";
		db.query(str,function(err,items){ 
			if(err) {
				console.log(err);
				return; 
			} 
	        var end = Date.now();
            if(items){
                console.log("Time to find "+items.length+" "+type+" entries : " + (end - start));

			if(callback!==undefined && callback!==null)
		    	return callback(items); 
			}
		}); 

	}, 
	join : function(db, callback){
 
		console.log("Starting join");
 
		var start = Date.now();   

	    var str = 	_q(["SELECT restaurants.type FROM addresses, restaurants ",
	    				"WHERE restaurants.address=addresses.id "]); 
		db.query(str,function(err,items){ 
			if(err) {
				console.log(err);
				return; 
			} 
	        var end = Date.now();
            if(items){
                console.log("Time to join "+items.length+" entries : " + (end - start));

			if(callback!==undefined && callback!==null)
		    	return callback(items); 
			}
		});
	}
}



db.connect(function(err){

	if(err){ 
		return console.log('Error connecting to Db'); 
	} 

	var operation = 0;
	if(operation==0){//insert 

		//erase return, needed it in case i accidentally erase my db
		TestDB.remove(db,function(){

			var limit;
 
			limit = 100000;
			/*
			Time to insert 100 small entries :    10855
			Time to insert 1000 small entries :   84654
			Time to insert 10000 small entries :  1127430 [guessing 10x longer] 
			Time to insert 100000 small entries : 11274300 [guessing 10x longer]  

 			Time to insert 100 medium entries :    13888
 			Time to insert 1000 medium entries :   101518
 			Time to insert 10000 medium entries :  1388800 [guessing here. didn't try, too long]
 			Time to insert 100000 medium entries : 13888000 [guessing here. didn't try, too long]
			*/  

			
			TestDB.insert(db,limit,"small",{
				restaurant:{
				    "type": "small",
				    "borough": "Manhattan",
				    "cuisine": "Italian",
				    "text": "big text n stuff ",
				},
			    address: {
			        "street": "2 Avenue",
			        "zipcode": "10075",
			        "building": "1480", 
			    },
			}, function(){
				TestDB.insert(db,limit,"medium",{
					restaurant:{
					    "type": "medium",
					    "borough": "Manhattan ManhattanManhattan ManhattanManhattan ManhattanManhattan Manhattan",
					    "cuisine": "Italian ItalianItalian ItalianItalian ItalianItalian ItalianItalian Italian",
					    "text": "big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff big text n stuff ",
					},
				    address: {
				        "street": "2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue2 Avenue",
				        "zipcode": "10075",
				        "building": "1480148014801480148014801480148014801480148014801480", 
				    },
				}, function(){
					
					//guessing not much different, but not trying it because we won't go with MySQL
				});
			});
		});

	}else if(operation==1){//simple lookup

		/*
		[if we fetch only 1 field] 
		[Note, caching speeds up lookup, even after db connection is dropped]
		Time to find 1000 small entries : 69 
		Time to find 1000 medium entries : 8 
		Time to find 1000 small entries : 3 
		Time to find 1000 medium entries : 4 
		Time to find 1000 small entries : 3  
		Time to find 1000 medium entries : 2

		*/

		TestDB.find(db, "small", function(){
			TestDB.find(db, "medium", function(){
				TestDB.find(db, "small", function(){
					TestDB.find(db, "medium", function(){
						TestDB.find(db, "small", function(){
							TestDB.find(db, "medium", function(){

							});
						}); 
					});
				}); 
			});
		});
  
	}else if(operation==2){

		/* 
		Time to join 2000 entries : 221 
		*/ 

		TestDB.join(db, function(){

			console.log("done with join");
		}); 
	}
});