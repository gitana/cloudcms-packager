var fs = require("fs");
var path = require("path");

var archiver = require("archiver");
var extractZip = require("extract-zip");

var exports = module.exports;

var trim = exports.trim = function(text)
{
    var trimmed = text;

    if (trimmed && typeof(text) == "string")
    {
        trimmed = trimmed.replace(/^\s+|\s+$/g, '');
    }

    return trimmed;
};

var isArray = exports.isArray = function(a)
{
    return (!!a) && (a.constructor === Array);
};

var isObject = exports.isObject = function(a)
{
    return (!!a) && (a.constructor === Object);
};

var isString = exports.isString = function(a)
{
    return typeof(a) === "string";
};

var zip = exports.zip = function(directoryPath, zipFilePath, callback)
{
    var _err = null;

    var writableStream = fs.createWriteStream(zipFilePath);
    writableStream.on('close', function() {
        callback(_err);
    });

    var archive = archiver("zip");
    archive.on('error', function(err){
        _err = err;
    });

    archive.pipe(writableStream);

    archive.directory(directoryPath, false);

    archive.finalize();
};

var unzip = exports.unzip = function(zipFilePath, destinationFolderPath, callback)
{
    extractZip(zipFilePath, {dir: destinationFolderPath}, function (err) {
        callback(err);
    });
};
