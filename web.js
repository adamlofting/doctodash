var async = require('async');
var csv = require('csv');
var request = require('request');
var validTeams = require('./validteams.js').validTeams;

var NodeCache = require( "node-cache" );
var myCache = new NodeCache();

var express = require("express");
var app = express();

var GDOC_URL = 'https://docs.google.com/spreadsheets/d/1NdHzBIDKduAu6-vvQx6iy-93zCzAqUKk4x_eanDxv1A/export?format=csv&id=1NdHzBIDKduAu6-vvQx6iy-93zCzAqUKk4x_eanDxv1A&gid=1977400704';

function toInt(s) {
  if (s) {
    s = s.replace(/,/g, "");
  }
  var i = parseInt(s);
  if (!i) { i = 0; }
  return i;
}

function processCSV(team, fetchedCSV, callback) {
  var output = [];
  var colActive = team + '_active';
  var colNew = team + '_new';

  function addToOutput(date, activeCount, newCount) {
    activeCount = toInt(activeCount);
    newCount = toInt(newCount);
    var row = {
      'wkcommencing': date,
      'totalactive': activeCount,
      'new': newCount
    };
    output.push(row);
  }

  csv()
    .from.string(fetchedCSV, {
      columns: true,
      delimiter: ',',
      escape: '"',
    })
    .to.stream(process.stdout, {
      columns: ['date', colActive, colNew]
    })
    .transform(function (row) {
      if (row.date) {
        addToOutput(row.date, row[colActive], row[colNew]);
      }
      //return row;
    })
    .on('end', function (count) {
      // when writing to a file, use the 'close' event
      // the 'end' event may fire before the file has been written
      callback(null, output);

    })
    .on('error', function (error) {
      console.log(error.message);
      callback(null);
    });
}

function importCSV (team, callback) {
  // get the latest from Google
  request.get(GDOC_URL,
    function (err, res, body) {
      if (!err && res.statusCode === 200) {
        var csv = body;
        processCSV(team, csv, function processedCSV(err, res) {
          if (err) {
            console.log(err);
            callback(err);
          }
          callback(null, res);
        });
      } else {
        console.log("Error fetching Google Doc");
        console.log(err);
        console.log(res.statusCode);
        callback(null);
      }
    }
  );
}


function getData (team, callback) {
  // timer to check impact of loading
  console.time('getTeam');

  // check cache
  var cache = myCache.get(team);

  // check if anythign is saved in the cache
  if (cache[team]) {
    // Yes, use the cached list
    console.log('loaded from cache');
    console.timeEnd('getTeam');

    callback(null, cache[team]);

  } else {
    // No, get this from gdocs
    console.log('loading from google docs');

    importCSV(team, function (err, result) {
      if (err) {
        console.log(err);
        return callback(err);
      }
      console.timeEnd('getTeam');
      myCache.set(team, result, 600 ); // 10 mins
      callback(null, result);
    });
  }
}


function isValidTeamName (team) {
  if (validTeams.indexOf(team) !== -1) {
    return true;
  }
  return false;
}

// allow CORS so this can be graphed elsewhere in JS
app.all('*', function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "X-Requested-With");
  next();
});

// default gets all mozilla
app.get('/', function (req, res) {
  getData('all', function (err, result) {
    res.json(result);
  });
});

// team specific routes
app.get('/:team', function (req, res) {
  var team = req.params.team;
  if (isValidTeamName(team)) {
    getData(team, function (err, result) {
      res.json(result);
    });
  } else {
    res.json({
      error: 'invalid team name',
      try_one_of_these_instead: validTeams
    });
  }
});


var port = Number(process.env.PORT || 5000);
app.listen(port, function () {
  console.log("Listening on " + port);
});
