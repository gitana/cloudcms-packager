var fs = require("fs");
var parseString = require("xml2js").parseString;

// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "soccer-leagues",
    "archiveName": "example1"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    // package up the "my:league" content type definition
    packager.addFromDisk("./types/my_league/node.json");

    // parse XML and create content instances
    var xmlText = fs.readFileSync("./data/soccer-leagues.xml").toString();
    parseString(xmlText, function (err, result) {

        var array = result.root.leagues[0].league;
        for (var i = 0; i < array.length; i++)
        {
            var entry = JSON.parse(JSON.stringify(array[i]["$"]));
            entry._type = "my:league";
            entry.title = entry.name;
            delete entry.name;

            packager.addNode(entry);
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

