var async = require('async');
var csv = require('csv');
var request = require('request');
var validViews = require('./validteams.js').validViews;
var validTableauViews = require('./validteams.js').validTableauViews;
var dates = require('./dates.js');

var NodeCache = require( "node-cache" );
var myCache = new NodeCache();

var express = require("express");
var app = express();

// var GDOC_URL_ORIGINAL = 'https://docs.google.com/spreadsheets/d/1NdHzBIDKduAu6-vvQx6iy-93zCzAqUKk4x_eanDxv1A/export?format=csv&id=1NdHzBIDKduAu6-vvQx6iy-93zCzAqUKk4x_eanDxv1A&gid=1977400704';
var GDOC_URL_TABLEAU = 'https://docs.google.com/spreadsheets/d/1NdHzBIDKduAu6-vvQx6iy-93zCzAqUKk4x_eanDxv1A/export?format=csv&id=1NdHzBIDKduAu6-vvQx6iy-93zCzAqUKk4x_eanDxv1A&gid=1758040737';


/**
 * UTILS
 */
function toInt(s) {
  if (s) {
    s = s.replace(/,/g, "");
  }
  var i = parseInt(s);
  if (!i) { i = 0; }
  return i;
}

function isValidViewName (view) {
  if (validViews.indexOf(view) !== -1) {
    return true;
  }
  return false;
}

function isValidTableauViewName (view) {
  if (validTableauViews.indexOf(view) !== -1) {
    return true;
  }
  return false;
}

function transformNameToCompare (s) {
  s = s.toLowerCase();
  s = s.replace( /[^a-z]/g, '' );
  return s;
}

/**
 * CSV
 */

function processTableauCSV(view, fetchedCSV, callback) {
  var output = dates.weeksOutputTemplate();

  function addToOutput(date, activeCount, newCount) {
    activeCount = toInt(activeCount);
    newCount = toInt(newCount);
    for (var i = 0; i < output.length; i++) {
      if(output[i].wkcommencing === date) {
        output[i].totalactive = activeCount;
        output[i].new = newCount;
      }
    }
  }

  csv()
    .from.string(fetchedCSV, {
      columns: true,
      delimiter: ',',
      escape: '"',
    })
    .to.stream(process.stdout, {
      columns: ['view', 'date', 'active', 'new']
    })
    .transform(function (row) {
      // store the rows relevant to this view in output
      if (row.date) {
        if (row.view) {
          if (view === transformNameToCompare(row.view)) {
            addToOutput(row.date, row.active, row.new);
          }
        }
      }
    })
    .on('end', function (count) {
      callback(null, output);
    })
    .on('error', function (error) {
      console.log(error.message);
      callback(null);
    });
}

function importTableauCSV (view, callback) {
  // get the latest from Google
  request.get(GDOC_URL_TABLEAU,
    function (err, res, body) {
      if (!err && res.statusCode === 200) {
        var csv = body;
        processTableauCSV(view, csv, function processedCSV(err, res) {
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

/**
 * DATA
 */

function getDataTableau (view, callback) {
  // timer to check impact of loading
  console.time('getView');
  var cacheKey = view + '_tab';

  // check cache
  var cache = myCache.get(cacheKey);

  // check if anythign is saved in the cache
  if (cache[cacheKey]) {
    // Yes, use the cached list
    console.log('loaded from cache');
    console.timeEnd('getView');

    callback(null, cache[cacheKey]);

  } else {
    // No, get this from gdocs
    console.log('loading from google docs');

    importTableauCSV(view, function (err, result) {
      if (err) {
        console.log(err);
        return callback(err);
      }
      console.timeEnd('getView');
      myCache.set(cacheKey, result, 600 ); // 10 mins
      callback(null, result);
    });
  }
}

/**
 * ROUTING
 */

// allow CORS so this can be graphed elsewhere in JS
app.all('*', function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "X-Requested-With");
  next();
});

// default gets all mozilla
app.get('/', function (req, res) {
  getDataTableau('all', function (err, result) {
    res.json(result);
  });
});

// view specific routes
app.get('/tab/:view', function (req, res) {
  var view = req.params.view;
  if (isValidTableauViewName(view)) {
    getDataTableau(view, function (err, result) {
      res.json(result);
    });
  } else {
    res.json({
      error: 'invalid view name',
      try_one_of_these_instead: validTableauViews
    });
  }
});

/**
 * START THE SERVER
 */

var port = Number(process.env.PORT || 5000);
app.listen(port, function () {
  console.log("Listening on " + port);
});
