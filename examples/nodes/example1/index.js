// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "nodes",
    "archiveName": "example1"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    // package up content type definitions
    packager.addFromDisk("./types/my_article/node.json");
    packager.addFromDisk("./types/my_author/node.json");
    packager.addFromDisk("./types/my_authored_by/node.json");

    // package up articles and authors
    packager.addFromDisk("./data/authors.json", "my:author");
    packager.addFromDisk("./data/articles.json", "my:article");

    // add attachments for Daenerys and Jon
    packager.addAttachment("dt", "default", "./data/daenerys_targaryen.jpg");
    packager.addAttachment("js", "default", "./data/jon_snow.jpg");

    // commit
    packager.package(function(err, archiveInfo) {

        // err if the packager failed to commit
        if (err) {
            return console.error(err);
        }

    });

});

