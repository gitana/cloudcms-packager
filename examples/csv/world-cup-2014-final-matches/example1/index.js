var csv = require("csv");
var fs = require("fs");

// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "world-cup-2014-final-matches",
    "archiveName": "example1"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    // parse CSV
    var csvText = fs.readFileSync("./data/2014-world-cup-final-matches.csv").toString();
    csv.parse(csvText, function (err, array) {

        // first row is the header row
        var header = array[0];

        // auto-generate the "my:match" type from this
        var contentTypeObject = {
            "title": "Match",
            "type": "object",
            "properties": {
                "title": {
                    "title": "Title",
                    "type": "string"
                }
            },
            "_qname": "my:match",
            "_type": "d:type",
            "_parent": "n:node"
        };
        for (var z = 0; z < header.length; z++)
        {
            contentTypeObject.properties[header[z]] = {
                "title": header[z],
                "type": "string"
            };
        }
        packager.addNode(contentTypeObject);

        // generate all of the content instances
        for (var i = 1; i < array.length; i++)
        {
            var entry = JSON.parse(JSON.stringify(array[i]));

            var contentInstanceObject = {};
            contentInstanceObject._type = "my:match";
            contentInstanceObject.title = entry[0];

            for (var z = 0; z < header.length; z++)
            {
                contentInstanceObject[header[z]] = entry[z];
            }

            packager.addNode(contentInstanceObject);
        }

        // commit
        packager.package(function(err, archiveInfo) {

            // err if the packager failed to commit
            if (err) {
                return console.error(err);
            }

        });
    });
});

