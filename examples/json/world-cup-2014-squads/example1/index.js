// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "worldcup2014squads",
    "archiveName": "example1"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    // load in content from JSON
    var array = require("./data/2014-world-cup-squads.json");
    for (var i = 0; i < array.length; i++)
    {
        var obj = JSON.parse(JSON.stringify(array[i]));

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

