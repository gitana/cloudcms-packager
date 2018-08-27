// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "worldcup2014squads",
    "archiveName": "example2"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    // package up the "my:athlete" content type definition
    packager.addFromDisk("./types/my_athlete/node.json");

    // load in content from JSON
    var array = require("./data/2014-world-cup-squads.json");
    for (var i = 0; i < array.length; i++)
    {
        var obj = JSON.parse(JSON.stringify(array[i]));
        obj._type = "my:athlete";

        packager.addNode(obj);
    }

    // commit
    packager.package(function(err, archiveInfo) {

        // err if the packager failed to commit
        if (err) {
            return console.error(err);
        }

    });

});

