// Indexes we'll need
function setupIndexes() {
	db.Movie.ensureIndex({SeriesID: 1});
	db.Movie.ensureIndex({MovieID: 1}, {unique:true});
	db.MovieDoc.ensureIndex({MovieID: 1});
	db.Contributor.ensureIndex({ContribName: 1}, {unique:true});
	db.MovieRole.ensureIndex({IsGenuine: 1});
	db.MovieRole.ensureIndex({MovieID: 1});
	db.MovieRole.ensureIndex({ContribName: 1});
	db.MovieRole.ensureIndex({ContribClass: 1});
	db.MovieRole.ensureIndex({MovieID: 1, ContribName: 1, ContribClass: 1}, {unique:true});
}

// Print the episdode name and release year for the specified series.
// Example: "\"The Good Wife\" (2009)"
function getEpisodes(seriesName) {
	print ("Episodes of " + seriesName);
	var cursor = db.Movie.find({SeriesID:seriesName, SeriesType:"E"}, {_id : 0, MovieID : 1, ReleaseYear: 1});
	while (cursor.hasNext()) {
		printjson(cursor.next());
	}
}

// Print details of the contributor and his/her movie roles
// Example: "Hanks, Tom"
function getContribDetailsAndFilmography(contribName) {
	print ("Details for Contributor " + contribName);
	printjson(db.Contributor.findOne({ContribName: contribName}));
	print ("Movie roles:");
	var cursor = db.MovieRole.find({ContribName: contribName}, {ContribName: 0, _id: 0}).sort({ContribClass: 1});
	while (cursor.hasNext()) {
		printjson(cursor.next());
	}
}

// Get details of specified movie, count per doc type, and full cast
// Example: "Goodfellas (1990)"
function getMovieDetailsCastAndDocCount(movieID) {
	print ("Details for " + movieID);
	printjson(db.Movie.findOne({MovieID: movieID}));
	print ("Movie Doc Counts");
	var dcursor = db.MovieDoc.aggregate([
		{$match: {MovieID: movieID}},
		{$group: {_id: "$DocType", count: {$sum: 1}}}
	]);
	while (dcursor.hasNext()) {
		printjson(dcursor.next());
	}
	print ("Cast:");
	var ccursor = db.MovieRole.find({MovieID: movieID}, {MovieID: 0, _id: 0}).sort({ContribClass: 1});
	while (ccursor.hasNext()) {
		printjson(ccursor.next());
	}
}

// Get movie count per genre
function getCountGenres() {
	print("Genres All Time");
	var cursor = db.Movie.aggregate([
		{$unwind : "$Genres" },
		{$group : { _id : "$Genres" , count : { $sum : 1 } } },
		{$sort : { count: -1 }}
	]);
	while (cursor.hasNext()) {
		printjson(cursor.next());
	}
}

// Get movie count per genre per year
function getCountGenresPerYear() {
	print("Genres Per Year");
	var cursor = db.Movie.aggregate([
		{$unwind : "$Genres" },
		{$group : { _id : {ReleaseYear: "$ReleaseYear", Genres: "$Genres"} , count : { $sum : 1 } } },
		{$sort : { "_id.ReleaseYear" :  -1} }
	]);
	while (cursor.hasNext()) {
		printjson(cursor.next());
	}
}

// Get movie count per genre for the specified contributor. Only include Feature filmsn.
function getCountGenresByContributor(contribName) {

	print ("Getting contrib movie list");
	var cursor = db.MovieRole.find({ContribName:contribName}, {_id:0, MovieID:1});
	var arrMovies = new Array();
	cursor.forEach(function(role) {
		if (arrMovies.indexOf(role.MovieID < 0)) {
			arrMovies.push(role.MovieID);
		}
	});

	print ("Determining Genres");
	var cursor = db.Movie.aggregate([
		{$match: {SeriesType : "F", MovieID : {$in : arrMovies}}},
		{$project: {MovieID: "$MovieID", SeriesType: "$SeriesType", Genres: "$Genres"}},
		{$unwind : "$Genres" },
		{$group : { _id : "$Genres" , count : { $sum : 1 } } },
		{$sort : { count: -1 }}
	]);
	while (cursor.hasNext()) {
		printjson(cursor.next());
	}
}

// Get N contributors with the most spouses
function getNContributorsWithMostSpouses(n) {
	print("Contribs With Most Spouses	");
	var cursor = db.Contributor.aggregate([
		{$unwind : "$PersonData.ContribSpouses" },
		{$project: {ContribName: "$ContribName", PersonData: "$PersonData"}},
		{$group : { _id : "$ContribName" , count : { $sum : 1 } } },
		{$sort : { count: -1 }},
		{$limit: n}
	]);
	while (cursor.hasNext()) {
		printjson(cursor.next());
	}
}

//
// Kevin Back

// Oops - ETL output running time as string; convert to int
function fixRunningTime() {
	db.Movie.find().forEach(function (x) {
		x.RunningTime = parseInt(x.RunningTime);
		db.Movie.save(x);
	});
}

// For Kevin Bacon game, we want to consider only "genuine" movies.
// A genuine movie has is a feature, is 80 minutes long or longer,
// and isn't TV movie, video, or video game
function assertMovieIsGenuine() {
	print("Resetting");
	printjson(db.Movie.update({IsGenuine: true}, {$set: {IsGenuine: false}}, {multi:true }));
	printjson(db.MovieRole.update({IsGenuine: true}, {$set: {IsGenuine: false}}, {multi:true }));

	// set movie is genuine flag
	print ("Updating Movie");
	printjson(db.Movie.update(
			{SeriesType: "F", RunningTime: {$gte: 80}, $where:
				function() {
					return this.MovieID.indexOf("(TV)") < 0 && this.MovieID.indexOf("(V)") < 0 && this.MovieID.indexOf("(VG)") < 0 ;
				}
			},
			{$set: {IsGenuine: true}},
			{multi: true}));

	// now propagate to MovieRole
	var cursor = db.Movie.find({IsGenuine: true}).addOption(DBQuery.Option.noTimeout);
	var count = 0;
	var roles = 0;
	while(cursor.hasNext()) {
		var next = cursor.next();
		count++;
		if ((count % 1000) == 0) print("now at count " + count + " remaining " + cursor.objsLeftInBatch() + " roles " + roles);
		roles += db.MovieRole.update({MovieID: next.MovieID, ContribClass: {$in: ['actors', 'actresses']}}, {$set: {IsGenuine: true}}, {multi:true}).nModified;
	}
	print ("Roles modified " + roles);
}

// Helper function for buildAdjLists
// for actors I in actorsPerMovie, add the following list to ContribAdj: (lastMovie, actorsPerMovie(J)
function buildAdjListsUpdateContrib(lastMovie, actorsPerMovie) {
	for (var i = 0; i < actorsPerMovie.length; i++) {
		var currActorAdj = new Array();
		for (var j = 0; j < actorsPerMovie.length; j++) {
			if (i == j) continue;
			currActorAdj.push({ContribName: actorsPerMovie[j], MovieID: lastMovie});
		}
		// for actor i, add record to ContribAdj (either create or update it) with AdjList appended with currActorAdj array.
		db.ContribAdj.update({_id:actorsPerMovie[i]}, {$push: {AdjList: {$each: currActorAdj}}}, {upsert:true});
	}
}

// Well graph algorithms need adjency lists. Let's try to build such a list for the whole graph of actors and actresses in geuine movies.
// Write to ContribAdj
function buildAdjLists() {
	print(new Date() + " getting the movie-role list");
	var cursor = db.MovieRole.find({ContribClass: { $in: ['actors', 'actresses']}, IsGenuine: true}, {_id:0, MovieID:1, MovieID:1, ContribName:1}).sort({MovieID: 1}).addOption(DBQuery.Option.noTimeout);

	// builder assumes resultset is ordered by movieid, as above query ensures
	print(new Date() + " build each list per actor " + cursor.count());
	var lastMovie = "";
	var actorsPerMovie = new Array();
	var count = 0;
	while(cursor.hasNext()) {
		var next = cursor.next();
		count++;
		if ((count % 1000) == 0) print("now at count " + count + " remaining " + cursor.objsLeftInBatch() + " movie " + lastMovie);

		if (next.MovieID == lastMovie) {
			actorsPerMovie.push(next.ContribName);
			//print("Still on *" + lastMovie + "* at " + count);
		}
		else {
			//print("Change from *" + lastMovie + "* to *" + next.MovieID + "* at " + count);

			buildAdjListsUpdateContrib(lastMovie, actorsPerMovie);

			// start again
			lastMovie = next.MovieID;
			actorsPerMovie = null;
			actorsPerMovie = new Array();
			actorsPerMovie.push(next.ContribName);
		}
	}

	if (actorsPerMovie.length > 0) {
		buildAdjListsUpdateContrib(lastMovie, actorsPerMovie);
	}
}

// OK, let's play the Kevin Bacon game!!!
// Find shortest path from specified source contrib to other actors/actresses
// Write results to specified collection. Use a different collection for each source contrib.
// When it's done, I can check path from source to dest with a query like
// db[collectionName].findOne(_id: DestContrib})
// find6dPathFromSource("Bacon, Kevin (I)", "BaconPaths", 6);
function find6dPathFromSource(sourceContribName, collectionName, maxDegree) {
	// might have run this previously; clean up
	print("Cleaning up " + sourceContribName);
	db[collectionName].drop();
	db[collectionName].ensureIndex({"value.NeedsAdj":1});

	// insert initial record
	print("Inserting source record " + sourceContribName);
	db[collectionName].insert({_id: sourceContribName, value: {Distance: 0, Path: [], NeedsAdj: true, AdjList: []} } );

	// Look maxDegree (e.g., 6) times (
	for (var i = 0; i < maxDegree; i++) {

		// pre-step: need to add adjlist to the MR record because MR won't allow us to lookup ContribAdj.
		// it breaks the sharding design
		var cursor = db[collectionName].find({"value.NeedsAdj":true}).addOption(DBQuery.Option.noTimeout);
		while(cursor.hasNext()) {
			var next = cursor.next();
			var adjList = db.ContribAdj.findOne({_id:  next._id}, {_id:0,AdjList:1}).AdjList;
			db[collectionName].update({_id:next._id}, {$set: {"value.NeedsAdj":false, "value.AdjList": adjList}});
		}

		print(new Date() + " Iter " + i + " running MR");
		runMRNextHop(collectionName);

		// get a count of distances so far
		print(new Date() + " Iter " + i + " distance summary");
		var sumcursor = db[collectionName].aggregate([
			{$group : { _id : "$value.Distance" , count : { $sum : 1 } } },
			{$sort : { count: -1 }}
		]);
		while(sumcursor.hasNext()) {
			printjson(sumcursor.next());
		}
	}
}

// Here's the MR algorithm, a single-source shortest path
function runMRNextHop(collectionName) {
	db[collectionName].mapReduce(
		// mapper
		function() {

			if (this.value.AdjList.length > 0) {
				// emit a record for each adj item
				for (var i = 0; i < this.value.AdjList.length; i++) {
					var adjPath = JSON.parse(JSON.stringify(this.value.Path));
					adjPath.push({ContribName:this._id, MovieID:this.value.AdjList[i].MovieID});
					emit(this.value.AdjList[i].ContribName, {Distance: this.value.Distance+1, Path: adjPath, NeedsAdj: true, AdjList: []});
				}
				this.value.NeedsAdj = false;
				this.value.AdjList = [];
			}

			// emit me too
			emit(this._id, this.value);
		},

		// reducer
		function(key, values) {
			// pretty simple; take the mininum distance for specified key
			var minDist = 9999;
			var valueToUse = null;
			for (var i = 0; i < values.length; i++) {
				if (values[i].Distance <= minDist) {
					valueToUse = values[i];
					minDist = values[i].Distance;
				}
			}
			valueToUse.Trace = values.length + " " + minDist;
			return valueToUse;
		},
		{
			out: collectionName
		}
	);
}
