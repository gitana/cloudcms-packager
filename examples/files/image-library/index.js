var walk = require("fs-walk");
var fs = require("fs");
var path = require("path");

// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "files",
    "archiveName": "sample"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    var imageCount = 0;
    walk.walkSync("./images", function(basedir, filename, stat) {

        var alias = "image-" + (imageCount++);
        var obj = {
            "title": filename,
            "_alias": alias
        };
        packager.addNode(obj);

        packager.addAttachment(alias, "default", path.join(basedir, filename));
    });

    // commit
    packager.package(function(err, archiveInfo) {

        // err if the packager failed to commit
        if (err) {
            return console.error(err);
        }

    });

});

