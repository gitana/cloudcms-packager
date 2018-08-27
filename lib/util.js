var fs = require("fs");
var path = require("path");

var archiver = require("archiver");
var extractZip = require("extract-zip");
var uuidv4 = require("uuid/v4");

var exports = module.exports;

// ECMAScript 6 includes this function, but we can declare here in case it isn't available
if (!String.prototype.endsWith) {
    String.prototype.endsWith = function(searchString, position) {
        var subjectString = this.toString();
        if (typeof position !== 'number' || !isFinite(position) || Math.floor(position) !== position || position > subjectString.length) {
            position = subjectString.length;
        }
        position -= searchString.length;
        var lastIndex = subjectString.indexOf(searchString, position);
        return lastIndex !== -1 && lastIndex === position;
    };
}

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

var randomGUID = exports.randomGUID = function()
{
    var val = uuidv4();
    val = replaceAll(val, "-", "");
    val = val.toLowerCase();
    return val.substring(0, 20);
};

var replaceAll = exports.replaceAll = function(text, find, replace)
{
    var i = -1;
    do
    {
        i = text.indexOf(find);
        if (i > -1)
        {
            text = text.substring(0, i) + replace + text.substring(i + find.length);
        }
    } while (i > -1);

    return text;
};

