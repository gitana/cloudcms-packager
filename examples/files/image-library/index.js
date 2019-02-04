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

    // Add Image content type
    var contentTypeObject = {
        "title": "Image",
        "type": "object",
        "properties": {
            "title": {
                "title": "Title",
                "type": "string"
            }
        },
        "_qname": "my:image",
        "_type": "d:type",
        "_parent": "n:node"
    };

    packager.addNode(contentTypeObject);

    var imageCount = 0;
    walk.walkSync("./images", function(basedir, filename, stat) {

        var alias = "image-" + (imageCount++);
        var obj = {
            "title": filename,
            "_alias": alias,
            "_type": "my:image"
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

