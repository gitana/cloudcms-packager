var path = require("path");

var fs = require("fs");
var readDirRecursive = require("fs-readdir-recursive");
var mime = require("mime");
var mkdirp = require('mkdirp');
var async = require("async");
var temp = require("temp");

var StringifyStream = require("stringifystream");
var Readable = require("stream").Readable;

var CompilerFactory = require("./compiler");
var util = require("./util");

var isString = util.isString;
var isArray = util.isArray;
var isObject = util.isObject;

var MAX_RECORDS_PER_ARCHIVE = 50000;

// for testing
//var MAX_RECORDS_PER_ARCHIVE = 100;

// hand back factory methods
module.exports.create = function(config, callback) {

    if (typeof(config) === "function") {
        callback = config;
        config = {};
    }

    var outputPath = config.outputPath;
    if (!outputPath) {
        outputPath = "./archives";
    }

    if (!config.archiveGroup) {
        config.archiveGroup = "packager";
    }
    if (!config.archiveName) {
        config.archiveName = "import";
    }
    if (!config.archiveVersion) {
        config.archiveVersion = "" + new Date().getTime();
    }

    var archiveGroup = config.archiveGroup.toLowerCase();
    var archiveName = config.archiveName.toLowerCase();
    var archiveVersion = config.archiveVersion.toLowerCase();

    // don't use temp for the moment since it helps to inspect the file cache (for now)
    var workingFolder = temp.mkdirSync("packager");
    mkdirp.sync(workingFolder + '/package');

    console.log("New package, working dir: " + workingFolder);

    // create a packager
    var packager = createPackager(".", workingFolder, outputPath, archiveGroup, archiveName, archiveVersion);

    callback(null, packager);

};

var report = function(text) {
    console.log("[" + new Date().getTime() + "] " + text);
};

var getPlatformId = function() {
    return "platformId";
};

var getRepositoryId = function() {
    return "repositoryId";
};

var branchId = "" + new Date().getTime();
var getBranchId = function() {
    return branchId;
};

var changesetRev = "0";
var changesetName = "" + new Date().getTime();
//var changesetName = "root";
var changesetId = changesetRev + ":" + changesetName;
var getChangesetRev = function() {
    return changesetRev;
};
var getChangesetId = function() {
    return changesetId;
};
var getChangesetFileName = function() {
    return changesetRev + "_" + changesetName;
};

var createPackager = function(basePath, workingFolder, outputPath, archiveGroup, archiveName, archiveVersion)
{
    // our compiler instance
    var compiler = CompilerFactory();

    // assume some values

    // auto-resolve the root
    var rootRecord = compiler.addObject("node", {
        "repositoryId": getRepositoryId(),
        "branchId": getBranchId(),
        "path": "/"
    }, {
        "_qname": "r:root",
        "_type": "n:root"
    });

    // common workhorse function for both _addNode and _addAssociation
    var _doAddNodeOrAssociation = function(type, object, alternateId)
    {
        var sourceBinding = {
            "repositoryId": getRepositoryId(),
            "branchId": getBranchId()
        };

        // if object has a "_key" field, we allow for auto-creation of "_existing" to match
        // this allows collisions to be detected on import and merges to naturally occur
        if (object && object._key && !object._existing) {
            object._existing = {
                "_key": object._key
            };
        }

        //console.log("Using alternate id: " + alternateId);
        return compiler.addObject(type, sourceBinding, object, [alternateId])
    };

    var _addNode = function(object, alternateId)
    {
        return _doAddNodeOrAssociation("node", object, alternateId);
    };

    var _addAssociation = function(sourceAlias, targetAlias, object, alternateId)
    {
        if (!object) {
            object = {};
        }

        if (sourceAlias.id) {
            sourceAlias = sourceAlias.id;
        }
        if (targetAlias.id) {
            targetAlias = targetAlias.id;
        }

        var sourceObject = compiler.getObject(sourceAlias);
        if (!sourceObject) {
            console.log("NULL SOURCE OBJECT: " + JSON.stringify(sourceAlias));
            console.trace();

        }
        object.source_type = sourceObject._type;
        object.source = sourceAlias;

        var targetObject = compiler.getObject(targetAlias);
        object.target_type = targetObject._type;
        object.target = targetAlias;

        if (!object._type) {
            object._type = "a:child";
        }

        if (!object.directionality) {
            object.directionality = "DIRECTED";
        }

        return _doAddNodeOrAssociation("association", object, alternateId);
    };

    var _addAttachment = function(alias, attachmentId, attachmentSource)
    {
        return compiler.addAttachment(alias, attachmentId, attachmentSource);
    };

    /**
     * Walks the directory structure of the "loader" format and parses things in preparation for writing into the
     * "transfer/zip" format.
     *
     * @param directoryName
     * @param callback
     *
     * @private
     */
    var _addDirectory = function(directoryName, callback)
    {
        var directoryPath = null;
        if (path.isAbsolute(directoryName)) {
            directoryPath = directoryName;
        } else {
            directoryPath = path.join(basePath, directoryName);
        }

        // all files under this path
        var files = readDirRecursive(directoryPath);
        for (var i = 0; i < files.length; i++)
        {
            var filename = path.basename(files[i]);
            if (filename.indexOf(".json") > -1)
            {
                var a = filename.indexOf(".json");
                var type = filename.substring(0, a);

                if (type === "node" || type === "association")
                {
                    var nodeFilePath = path.join(directoryPath, files[i]);
                    var nodeFolderPath = path.dirname(nodeFilePath);

                    // add the node (or association)
                    var nodeObject;
                    try {
                        nodeObject = JSON.parse("" + fs.readFileSync(nodeFilePath));
                    } catch(parseError) {
                        console.log("JSON parse failed for: " + nodeFilePath + "\n" + parseError);
                        throw parseError;
                    }
                    var nodeRecord = _doAddNodeOrAssociation(type, nodeObject, nodeFilePath.substring(directoryPath.length + 1));

                    // add any attachments
                    var nodeAttachmentsFolderPath = path.join(nodeFolderPath, 'attachments');
                    if (fs.existsSync(nodeAttachmentsFolderPath))
                    {
                        var attachmentFiles = fs.readdirSync(nodeAttachmentsFolderPath);
                        for (var j = 0; j < attachmentFiles.length; j++)
                        {
                            var attachmentFile = attachmentFiles[j];

                            var attachmentPath = path.join(nodeAttachmentsFolderPath, attachmentFile);
                            var attachmentId = path.basename(attachmentFile, path.extname(attachmentFile));

                            // add the attachment
                            _addAttachment(nodeRecord, attachmentId, attachmentPath);
                        }
                    }

                    // if the node is a definition, it may support forms
                    var nodeFormsFolderPath = path.join(nodeFolderPath, "forms");
                    if (fs.existsSync(nodeFormsFolderPath))
                    {
                        var formPaths = fs.readdirSync(nodeFormsFolderPath);
                        for (var j = 0; j < formPaths.length; j++)
                        {
                            var formPath = path.join(nodeFormsFolderPath, formPaths[j]);
                            var formKey = path.basename(formPaths[j], path.extname(formPaths[j]));

                            // add the form
                            var formObject = "" + fs.readFileSync(path.join(nodeFormsFolderPath, formPaths[j]));
                            formObject = JSON.parse(formObject);
                            formObject.engineId = "alpaca1";
                            formObject._type = "n:form";
                            var formRecord = _addNode(formObject);

                            // add the association between the node and the form
                            var formAssociationObject = {
                                "_type": "a:has_form",
                                "form-key": formKey,
                                "directionality": "DIRECTED"
                            };

                            _addAssociation(nodeRecord, formRecord, formAssociationObject, [formPath]);
                        }
                    }

                    // if the node has translations, add those in
                    var nodeTranslationsFolderPath = path.join(nodeFolderPath, 'translations');
                    if (fs.existsSync(nodeTranslationsFolderPath))
                    {
                        var localePaths = fs.readdirSync(nodeTranslationsFolderPath);
                        for (var j = 0; j < localePaths.length; j++)
                        {
                            var locale = path.basename(localePaths[j]);
                            var nodeTranslationFilePath = path.join(nodeTranslationsFolderPath, locale, "translation.json");
                            if (fs.existsSync(nodeTranslationFilePath))
                            {
                                // add the translation
                                var translationObject = "" + fs.readFileSync(nodeTranslationFilePath);
                                translationObject = JSON.parse(translationObject);
                                if (!translationObject._features) {
                                    translationObject._features = {};
                                }
                                translationObject._features["f:translation"] = {
                                    "enabled": true,
                                    "locale": locale,
                                    "edition": "1.0",
                                    "master-node-id": nodeRecord.id // alias to master
                                };
                                // for safety, remove f:multilingual from the translation
                                delete translationObject._features["f:multilingual"];
                                var translationRecord = _addNode(translationObject);

                                // associate the translation to the node
                                var translationAssociationObject = {
                                    "locale": locale,
                                    "edition": "1.0",
                                    "_type": "a:has_translation",
                                    "directionality": "DIRECTED"
                                };

                                _addAssociation(nodeRecord, translationRecord, translationAssociationObject, nodeTranslationFilePath);

                                // ensure that the master record is marked as multi-lingual
                                if (!nodeRecord.json._features) {
                                    nodeRecord.json._features = {};
                                }
                                nodeRecord.json._features["f:multilingual"] = {
                                    "enabled": true,
                                    "edition": "1.0"
                                };

                                // add in any translation attachments
                                var nodeTranslationAttachmentsPath = path.join(nodeTranslationsFolderPath, locale, "attachments");
                                if (fs.existsSync(nodeTranslationAttachmentsPath))
                                {
                                    // translation has attachments
                                    var attachmentPaths = fs.readdirSync(nodeTranslationAttachmentsPath);
                                    for (var j = 0; j < attachmentPaths.length; j++)
                                    {
                                        var attachmentPath = path.join(nodeTranslationAttachmentsPath, attachmentPaths[j]);
                                        var attachmentId = path.basename(attachmentPaths[j], path.extname(attachmentPaths[j]));

                                        // add the attachment
                                        _addAttachment(translationRecord, attachmentId, attachmentPath);
                                    }
                                }
                            }
                        }
                    }
                }
                else if (type === "workflow-model")
                {
                    var workflowModelFilePath = path.join(directoryPath, files[i]);

                    // add the node (or association)
                    var workflowModelObject = JSON.parse("" + fs.readFileSync(workflowModelFilePath));
                    //TODO
                    //var workflowModelRecord = _doAddWorkflowModel(workflowModelObject);
                }
            }
        }

        callback();
    };

    var _createContainerPaths = function(callback)
    {
        var recordsByPath = {};

        var doMkdirs = function(directoryPath)
        {
            if (!directoryPath) {
                return rootRecord;
            }

            // make sure we don't have a trailing /
            if (directoryPath.endsWith("/")) {
                directoryPath = directoryPath.substring(0, directoryPath.length - 1);
            }

            if (directoryPath === "." || directoryPath === "/" || directoryPath === "") {
                return rootRecord;
            }

            var containerRecord = recordsByPath[directoryPath];
            if (containerRecord) {
                return containerRecord;
            }

            // ensure parent is around
            containerRecord = doMkdirs(path.dirname(directoryPath));

            // create the child folder
            var childContainerObject = {
                "_type": "n:node",
                "_features": {
                    "f:container": {
                        "enabled": true
                    },
                    "f:filename": {
                        "filename": _sanitizeFilename(path.basename(directoryPath))
                    }
                },
                "title": path.basename(directoryPath)
            };
            var childContainerRecord = _addNode(childContainerObject);

            // add the association
            var associationObject = {
                "_type": "a:child",
                "directionality": "DIRECTED"
            };
            _addAssociation(containerRecord, childContainerRecord, associationObject);

            recordsByPath[directoryPath] = childContainerRecord;
            childContainerRecord.path = directoryPath;

            return childContainerRecord;
        };

        // create folder nodes, etc needed to complete the content graph
        compiler.eachRecord(function(alias, record) {

            var object = record.json;

            var containerPath = object._parentFolderPath;
            if (containerPath)
            {
                if (containerPath.indexOf('/') === 0) {
                    containerPath = containerPath.substring(1);
                }

                if (containerPath.endsWith("/")) {
                    containerPath = containerPath.substring(0, containerPath.length - 1);
                }

                var containerRecord = doMkdirs(containerPath);

                // add the association
                var associationObject = {
                    "_type": "a:child",
                    "directionality": "DIRECTED"
                };
                _addAssociation(containerRecord, record, associationObject);

                // if we happen to know the filename for this record, we can anticipate the path and store it
                var objectFilename = object._fileName;
                if (!objectFilename)
                {
                    objectFilename = object.filename;
                }
                if (!objectFilename)
                {
                    if (object._features && object._features["f:filename"] && object._features["f:filename"].filename)
                    {
                        objectFilename = object._features["f:filename"].filename;
                    }
                }
                if (!objectFilename && object.title)
                {
                    objectFilename = object.title;
                }
                if (!objectFilename)
                {
                    objectFilename = alias;
                }

                // store filename
                if (!object._features)
                {
                    object._features = {};
                }
                if (!object._features["f:filename"])
                {
                    object._features["f:filename"] = {};
                }
                object._features["f:filename"].filename = _sanitizeFilename(objectFilename);

                var filePath = path.join(containerPath, objectFilename);

                recordsByPath[filePath] = record;
                record.path = filePath;
            }
        });

        callback();
    };

    var _sanitizeFilename = function(text)
    {
        return text.replace(new RegExp("[^a-zA-Z0-9\._]+", "g"), "_");
    };

    var _parseReferences = function(callback)
    {
        var parseArray = function(array, ourId) {

            for (var i = 0; i < array.length; i++)
            {
                var val = array[i];

                if (isObject(val))
                {
                    parseObject(val, ourId);
                }
                else if (isArray(val))
                {
                    parseArray(val, ourId);
                }
                else if (isString(val))
                {
                    // if this is some we're referencing, either by alias or by file path, make sure that we
                    // touch it here to update it to the alias
                    var referencedRecord = compiler.getRecord(val);
                    if (referencedRecord)
                    {
                        // mark reference
                        compiler.addReference(referencedRecord.id, ourId);

                        // update to alias
                        if (val !== referencedRecord.id) {
                            array[i] = referencedRecord.id;
                        }
                    }
                }
            }
        };

        var parseObject = function(obj, ourId) {

            // collect keys
            var keys = [];
            for (var k in obj) {
                if (obj.hasOwnProperty(k)) {
                    keys.push(k);
                }
            }

            for (var i = 0; i < keys.length; i++)
            {
                var key = keys[i];

                var val = obj[key];

                if (isObject(val))
                {
                    parseObject(val, ourId);
                }
                else if (isArray(val))
                {
                    parseArray(val, ourId);
                }
                else if (isString(val))
                {
                    // if this is some we're referencing, either by alias or by file path, make sure that we
                    // touch it here to update it to the alias
                    var referencedRecord = compiler.getRecord(val);
                    if (referencedRecord)
                    {
                        // mark reference
                        compiler.addReference(referencedRecord.id, ourId);

                        // update to alias
                        if (val !== referencedRecord.id) {
                            obj[key] = referencedRecord.id;
                        }
                    }
                }
            }
        };

        var objects = compiler.getObjects();
        for (var alias in objects)
        {
            parseObject(objects[alias], alias);
        }

        callback();
    };

    var _resolveReferences = function(recordIds, callback)
    {
        var _substituteArray = function(array, prev, replacementText)
        {
            for (var i = 0; i < array.length; i++)
            {
                var val = array[i];

                if (isObject(val))
                {
                    _substituteObject(val, prev, replacementText);
                }
                else if (isArray(val))
                {
                    _substituteArray(val, prev, replacementText);
                }
                else if (isString(val))
                {
                    if (val === prev)
                    {
                        array[i] = replacementText;
                    }
                }
            }
        };

        var _substituteObject = function(obj, prev, replacementText)
        {
            // special case: related nodes
            _patchRelatedNode(obj, prev, replacementText);

            var keys = [];
            for (var k in obj)
            {
                if (obj.hasOwnProperty(k))
                {
                    keys.push(k);
                }
            }

            for (var i = 0; i < keys.length; i++)
            {
                var key = keys[i];

                var val = obj[key];

                if (isObject(val))
                {
                    _substituteObject(val, prev, replacementText);
                }
                else if (isArray(val))
                {
                    _substituteArray(val, prev, replacementText);
                }
                else if (isString(val))
                {
                    if (val === prev)
                    {
                        obj[key] = replacementText;
                    }
                }
            }
        };

        var _patchRelatedNode = function(obj, prev, replacementText)
        {
            var relatedId = obj["__related_node__"];
            if (relatedId && relatedId === prev)
            {
                var relatedObject = compiler.getObject(relatedId);
                if (relatedObject)
                {
                    delete obj["__related_node__"];

                    obj["id"] = relatedId;
                    obj["ref"] = "node://" + getPlatformId() + "/" + getRepositoryId() + "/" + getBranchId() + "/" + replacementText;
                    obj["qname"] = relatedObject._qname;
                    obj["typeQName"] = relatedObject._type;
                    obj["title"] = relatedObject.title ? relatedObject.title : replacementText;

                    report("Patched related node: " + relatedId + " to: " + obj.ref);
                    console.log(JSON.stringify(obj, null, "  "));
                }
                else
                {
                    report("ERROR while patching related node, cannot find related node for identifier: " + relatedId);
                }
            }
        };

        // collect all of the record aliases
        var recordAliases = [];
        compiler.eachRecord(function(alias, record) {
            recordAliases.push(alias);
        });

        report("Resolving " + recordAliases.length + " aliases");

        // walk through the record aliases and resolve
        var resolvedAliasIds = {};
        for (var i = 0; i < recordAliases.length; i++)
        {
            resolvedAliasIds[recordAliases[i]] = recordIds[i];
        }

        // now walk through all record aliases and substitute
        for (var i = 0; i < recordAliases.length; i++)
        {
            if (i % 1000 === 0) {
                report("Resolved: " + i + " of " + recordAliases.length + " aliases");
            }

            var recordAlias = recordAliases[i];
            var resolvedId = resolvedAliasIds[recordAlias];

            // are there any other objects that reference our alias?
            var recordReferences = compiler.getReferences(recordAlias);
            if (recordReferences.length > 0)
            {
                for (var j = 0; j < recordReferences.length; j++)
                {
                    var otherObject = compiler.getObject(recordReferences[j]);
                    if (!otherObject)
                    {
                        console.log("MISSING OBJECT FOR REF: " + recordReferences[j]);
                    }
                    else
                    {
                        _substituteObject(otherObject, recordAlias, resolvedId);
                    }
                }
            }

            compiler.resolveReference(recordAlias, resolvedId);
        }
        report("Resolved: " + recordAliases.length + " aliases");

        // support for simplified associations
        var records = compiler.getRecords();
        compiler.eachRecord(function(alias, record) {

            if (record.type === "association")
            {
                if (record.json.source && !record.json.source_type) {
                    record.json.source_type = records[record.json.source].json._type;
                }

                if (record.json.target && !record.json.target_type) {
                    record.json.target_type = records[record.json.target].json._type;
                }

                if (!record.json.directionality) {
                    record.json.directionality = "DIRECTED";
                }
            }

        });

        /*
        // report
        console.log("--------");
        var records = compiler.getRecords();
        for (var k in records)
        {
            if (records[k].type === "association")
            {
                console.log("S: " + JSON.stringify(records[k].json));
            }
        }
        */

        report("Resolution of aliases complete");

        // verify that everything resolved
        compiler.assertComplete(function(err) {

            if (err) {
                report("There was a problem during resolution assertion");
            } else {
                report("Compilation references and aliases resolved successfully");
            }

            callback(err);
        });
    };

    var _acquireGUIDs = function(idRequestCount) {

        var recordIds = [];

        if (idRequestCount > 0)
        {
            for (var i = 0; i < idRequestCount; i++)
            {
                recordIds.push(util.randomGUID());
            }
        }

        return recordIds;
    };

    var _bindAttachmentsToObjects = function(callback)
    {
        report("Binding attachments");

        // walk each record
        compiler.eachRecord(function(alias, record) {

            var object = record.json;

            for (var j = 0; j < record.attachments.length; j++)
            {
                var attachmentInfo = record.attachments[j];

                var attachmentId = attachmentInfo.id;
                var attachmentFilePath = attachmentInfo.path;

                // attachment properties
                var fileName = path.basename(attachmentFilePath);
                var fileSize = fs.statSync(attachmentFilePath)["size"];
                var mimeType = mime.getType(attachmentFilePath);

                if (!object._system) {
                    object._system = {};
                }
                if (!object._system.attachments) {
                    object._system.attachments = {};
                }
                object._system.attachments[attachmentId] = {
                    "contentType" : mimeType,
                    "length" : fileSize,
                    "objectId" : '',
                    "filename" : fileName
                };
            }
        });

        report("Completed binding of attachments");

        callback();
    };

    var _cleanupObjects = function(callback)
    {
        var err = false;

        report("Starting cleanup of objects");
        compiler.eachRecord(function(_doc, record) {

            var object = record.json;
            if (record.type === "node" || record.type === "association")
            {
                // ensure qname
                if (object._qname) {
                    if (object._qname === "o:" + object._alias) {
                        object._qname = "o:" + _doc
                    }
                }
                else {
                    object._qname = "o:" + object._doc;
                }

                if (!object._type) {
                    console.log("Object is missing _type: " + JSON.stringify(object));
                    err = true;
                }

                if (object._type === "d:type" && !object._parent) {
                    object._parent = "n:node";
                }

                if (object._type === "d:association" && !object._parent) {
                    object._parent = "a:linked";
                }
            }

            // strip off alias
            delete object._alias;

            // strip off path info
            delete object._parentFolderPath;
            delete object._fileName;
            delete object._filename;
            delete object._path;
        });

        report("Completed cleanup of objects");

        if (err) {
            return callback({
                "message": "One or more objects did not cleanup properly"
            });
        }

        callback();
    };

    var _verifyObjects = function(callback)
    {
        var err = false;

        var objectsByPath = {};

        report("Starting verification of objects");
        compiler.eachRecord(function(_doc, record) {

            var object = record.json;

            // assert that two objects are not claiming the same path
            if (record.path)
            {
                if (objectsByPath[record.path])
                {
                    console.log("Object claims a path: \"" + record.path + "\" that already exists, object: " + JSON.stringify(object, null, true) + " collides with: " + JSON.stringify(objectsByPath[record.path], null, true));
                    err = true;
                }

                objectsByPath[record.path] = object;
            }
        });

        report("Completed verification of objects");

        if (err) {
            return callback({
                "message": "One or more objects did not verify"
            });
        }

        callback();
    };

    var _commitToDisk = function(archiveRootFilePath, archiveGroup, archiveName, archiveVersion, traverser, callback)
    {
        report("Using temp location: " + archiveRootFilePath);

        // build out some structures that we know we'll need
        var platformId = getPlatformId();
        var repositoryId = getRepositoryId();
        var branchId = getBranchId();

        // ensure some folders are in place
        mkdirp.sync(archiveRootFilePath + '/platforms/' + platformId + '/repositories/' + repositoryId + '/branches/' + branchId);
        var nodesPath = archiveRootFilePath + '/platforms/' + platformId + '/repositories/' + repositoryId + '/changesets/' + getChangesetFileName() + '/nodes';
        mkdirp.sync(nodesPath);

        // write platform.json
        fs.writeFileSync(archiveRootFilePath + '/platforms/' + platformId + '/platform.json', JSON.stringify(_getPlatformObjectTemplate(), null, '  '));
        // write repository.json
        fs.writeFileSync(archiveRootFilePath + '/platforms/' + platformId + '/repositories/' + repositoryId + '/repository.json', JSON.stringify(_getRepositoryObjectTemplate(), null, '  '));
        // write branch.json
        fs.writeFileSync(archiveRootFilePath + '/platforms/' + platformId + '/repositories/' + repositoryId + '/branches/' + branchId + '/branch.json', JSON.stringify(_getBranchObjectTemplate(), null, '  '));
        // write changeset.json
        fs.writeFileSync(archiveRootFilePath + '/platforms/' + platformId + '/repositories/' + repositoryId + '/changesets/' + getChangesetFileName() + '/changeset.json', JSON.stringify(_getChangesetObjectTemplate(), null, '  '));

        // write all of the json down to disk
        traverser.eachRecord(function(_doc, record, c, max) {

            var json = _generateRecordJson(record);

            // write the json file
            var targetFilePath = path.join(archiveRootFilePath, "platforms", platformId, _generateRecordJsonFilePath(record));
            var targetFolderPath = path.dirname(targetFilePath);
            mkdirp.sync(targetFolderPath);
            fs.writeFileSync(targetFilePath, JSON.stringify(json, null, '  '));

            //console.log("ai: " + targetFilePath);

            if (c % 1000 === 0 || c === max - 1)
            {
                report("Wrote: " + (c + 1) + " of " + max + " objects");
            }
        });

        // for each record, copy over it's attachments
        traverser.eachRecord(function(_doc, record, c, max) {

            var targetNodeFilePath = path.join(archiveRootFilePath, "platforms", platformId, _generateRecordJsonFilePath(record));
            var targetNodeFolderPath = path.dirname(targetNodeFilePath);

            // walk over attachments
            for (var j = 0; j < record.attachments.length; j++)
            {
                //var attachmentId = record.attachments[j].id;
                var sourceAttachmentFilePath = record.attachments[j].path;

                var targetAttachmentsFolderPath = path.join(targetNodeFolderPath, "attachments");
                mkdirp.sync(targetAttachmentsFolderPath);

                var targetAttachmentFilePath = path.join(targetAttachmentsFolderPath, record.attachments[j].id) + path.extname(sourceAttachmentFilePath);

                // copy file sync
                fs.createReadStream(sourceAttachmentFilePath).pipe(fs.createWriteStream(targetAttachmentFilePath));
            }

            if (c % 1000 === 0 || c === max - 1)
            {
                report("Handled attachments for: " + (c + 1) + " of " + max + " objects");
            }
        });

        // write manifest
        var manifest = {};
        manifest["model"] = "2.0.0";
        manifest["group"] = archiveGroup;
        manifest["artifact"] = archiveName;
        manifest["version"] = archiveVersion;
        manifest["type"] = "branch";
        //manifest["dependencies"] = [];
        manifest["includes"] = [];
        manifest["tipChangesetOnly"] = true; // indicates that everything was compressed to a single changeset
        manifest["sources"] = [];
        manifest["contents"] = {};

        // write in platform
        var platformObject = {};
        platformObject["typeId"] = "platform";
        platformObject["id"] = getPlatformId();
        platformObject["key"] = "platform_" + getPlatformId();
        platformObject["location"] = "platforms/" + getPlatformId();
        platformObject["requiredBy"] = [];
        platformObject["requires"] = [{
            "typeId": "repository",
            "id": getRepositoryId(),
            "key": "repository_" + getRepositoryId()
        }];

        platformObject["dependencies"] = [{
            "typeId": "repository",
            "id": getRepositoryId(),
            "key": "repository_" + getRepositoryId()
        }];
        manifest.contents[platformObject.key] = platformObject;

        // write in repository
        var repositoryObject = {};
        repositoryObject["typeId"] = "repository";
        repositoryObject["id"] = getRepositoryId();
        repositoryObject["key"] = "repository_" + getRepositoryId();
        repositoryObject["location"] = "platforms/" + getPlatformId() + "/repositories/" + getRepositoryId();
        repositoryObject["requiredBy"] = [{
            "typeId": "platform",
            "id": getPlatformId(),
            "key": "platform_" + getPlatformId()
        }];
        repositoryObject["requires"] = [{
            "typeId": "branch",
            "id": getBranchId(),
            "key": "branch_" + getBranchId()
        }];
        repositoryObject["dependencies"] = [{
            "typeId": "branch",
            "id": getBranchId(),
            "key": "branch_" + getBranchId()
        }];
        manifest.contents[repositoryObject.key] = repositoryObject;

        // write in the branch
        var branchObject = {};
        branchObject["typeId"] = "branch";
        branchObject["id"] = getBranchId();
        branchObject["key"] = "branch_" + getBranchId();
        branchObject["location"] = "platforms/" + getPlatformId() + "/repositories/" + getRepositoryId() + "/branches/" + getBranchId();
        branchObject["requiredBy"] = [{
            "typeId": "repository",
            "id": getRepositoryId(),
            "key": "repository_" + getRepositoryId()
        }];
        branchObject["requires"] = [{
            "typeId": "changeset",
            "id": getChangesetId(),
            "key": "changeset_" + getChangesetId()
        }];
        branchObject["dependencies"] = [{
            "typeId": "changeset",
            "id": getChangesetId(),
            "key": "changeset_" + getChangesetId()
        }];
        manifest.contents[branchObject.key] = branchObject;

        // write in changeset
        var changesetObject = {};
        changesetObject["typeId"] = "changeset";
        changesetObject["id"] = getChangesetId();
        changesetObject["key"] = "changeset_" + getChangesetId();
        changesetObject["location"] = "platforms/" + getPlatformId() + "/repositories/" + getRepositoryId() + "/changesets/" + getChangesetFileName();
        changesetObject["requiredBy"] = [{
            "typeId": "branch",
            "id": getBranchId(),
            "key": "branch_" + getBranchId()
        }];
        changesetObject["requires"] = [];
        changesetObject["dependencies"] = [];
        traverser.eachRecord(function(_doc, record, c, max) {

            var dependencyObject = {};
            dependencyObject["typeId"] = record.type || "node";
            dependencyObject["id"] = record.id;
            dependencyObject["key"] = getChangesetId() + "_" + record.id;

            changesetObject["dependencies"].push(JSON.parse(JSON.stringify(dependencyObject)));
            changesetObject["requires"].push(JSON.parse(JSON.stringify(dependencyObject)));
        });
        manifest.contents[changesetObject.key] = changesetObject;

        // add to sources
        manifest.sources.push([{
            "typeId": "platform",
            "id": getPlatformId(),
            "key": "platform_" + getPlatformId(),
            "requires": [],
            "requiredBy": []
        }, {
            "typeId": "repository",
            "id": getRepositoryId(),
            "key": "repository_" + getRepositoryId(),
            "requires": [],
            "requiredBy": []
        }, {
            "typeId": "branch",
            "id": getBranchId(),
            "key": "branch_" + getBranchId(),
            "requires": [],
            "requiredBy": []
        }]);

        // walk the nodes
        traverser.eachRecord(function(_doc, record, c, max) {

            var entry = _generateRecordManifestEntry(record);

            // add to contents
            manifest.contents[entry.key] = entry;

            c++;

            if (c % 1000 === 0)
            {
                report("Generated manifest entries for: " + c + " of " + max + " objects");
            }
        });

        report("Writing manifest");

        var rs = Readable({objectMode: true});
        rs.push(manifest);
        rs.push(null);

        var manifestFileStream = fs.createWriteStream(path.join(archiveRootFilePath, "manifest.json"));
        manifestFileStream.on('error', function(err) {

            report("An error occurred on commit of objects to disk");

            callback(err);
        });
        manifestFileStream.on('finish', function(){

            report("Completed commit of objects to disk");

            callback();
        });

        rs.pipe(StringifyStream()).pipe(manifestFileStream);
    };

    var _generateRecordJson = function(record) {

        var json = record.json;

        // sync doc
        json._doc = record.id;

        if (record.type === "node" || record.type === "association")
        {
            // sync qname
            if (!json._qname)
            {
                json._qname = "o:" + json._doc;
            }

            // temp
            if (!json._system) {
                json._system = {};
            }
            json._system.changeset = getChangesetId();
        }

        return json;
    };

    var _generateRecordJsonFilePath = function(record) {

        var filePath = null;

        var binding = record.binding;

        if (record.type === "node")
        {
            filePath = "repositories/" + binding.repositoryId + "/changesets/" + getChangesetFileName() + "/nodes/" + record.id + "/node.json";
        }
        else if (record.type === "association")
        {
            filePath = "repositories/" + binding.repositoryId + "/changesets/" + getChangesetFileName() + "/nodes/" + record.id + "/association.json";
        }

        return filePath;
    };

    var _generateRecordManifestEntry = function(record) {

        var entry = {};

        entry.typeId = record.type;
        entry.id = record.id;
        entry.key = getChangesetId() + "_" + record.id;
        entry.requires = [];
        entry.requiredBy = [{
            "typeId": "changeset",
            "id": getChangesetId(),
            "key": "changeset_" + getChangesetId()
        }];
        entry.title = record.json.title ? record.json.title : record.json._doc;
        entry._qname = record.json._qname;
        entry._type = record.json._type;

        entry.location = "platforms/" + getPlatformId() + "/repositories/" + getRepositoryId() + "/changesets/" + getChangesetFileName() + "/nodes/" + entry.id;
        entry.dependencies = [];

        // path?
        if (record.path) {
            entry.path = record.path;
        }

        return entry;
    };

    var _createArchiveFile = function(zipSrcFolder, packageName, callback)
    {
        report("Creating archive file");

        // ensure we have an archives directory
        mkdirp.sync(outputPath);

        //var outputFileName = "./archives/" + packageName + ".zip";
        var outputFileName = path.join(outputPath, packageName + ".zip");

        // delete old file if it is there
        try { fs.unlinkSync(outputFileName); } catch (e) { }

        var zipFileName = path.join(basePath, outputFileName);
        util.zip(zipSrcFolder, zipFileName, function(err) {
            if (err) {
                return callback(err);
            }
            callback(null, outputFileName);
        });
    };

    var _getPlatformObjectTemplate = function()
    {
        return {
            "datastoreId" : getPlatformId(),
            "datastoreTypeId" : "platform",
            "_doc" :  getPlatformId()
        };
    };

    var _getRepositoryObjectTemplate = function()
    {
        return {
            "platformId" : getPlatformId(),
            "datastoreId" : getRepositoryId(),
            "datastoreTypeId" : "repository",
            "_doc" : getRepositoryId()
        };
    };

    var _getBranchObjectTemplate = function()
    {
        return {
            "_doc" : getBranchId(),
            "root" : "0:root",
            "tip" : getChangesetId(),
            "type": "CUSTOM"
        };
    };

    var _getChangesetObjectTemplate = function()
    {
        return {
            "_doc" : getChangesetId(),
            "revision" : getChangesetRev(),
            "branch" : getBranchId()
        };
    };


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////


    // what we hand back
    var r = {};

    /**
     * Adds the contents of a directory to the packager.
     *
     * For example,
     *
     *    packager.addDirectory("setup/data/core");
     *
     * @param directoryName
     */
    r.addDirectory = function(directoryName)
    {
        // adds the contents of a directory into the packager (i.e. /setup/data/core)
        _addDirectory(directoryName, function(err) {

            if (err)
            {
                throw err;
            }
        });
    };

    r.addNode = function(json)
    {
        if (!json._type) {
            json._type = "n:node";
        }

        return _addNode(json);
    };

    r.addAssociation = function(source, target, json, alternateId)
    {
        if (!json._type) {
            json._type = "a:linked";
        }

        return _addAssociation(source, target, json, alternateId);
    };

    r.addAttachment = function(_doc, attachmentId, attachmentSource)
    {
        return _addAttachment(_doc, attachmentId, attachmentSource);
    };

    r.getNodeWithType = r.getNodesWithType = function(type)
    {
        var results = {};

        var nodeRecords = compiler.getRecords("node");
        for (var _doc in nodeRecords)
        {
            var nodeRecord = nodeRecords[_doc];

            if (nodeRecord.json._type === type)
            {
                results[nodeRecord.id] = nodeRecord.json;
            }
        }

        var associationRecords = compiler.getRecords("association");
        for (var _doc in associationRecords)
        {
            var associationRecord = associationRecords[_doc];
            if (associationRecord.json._type === type)
            {
                results[associationRecord.id] = associationRecord.json;
            }
        }

        return results;
    };

    /**
     * Adds content form disk.  The content might be a JSON object where the object is a content instance or content type.
     * Or it may be a JSON array of such objects.
     */
    r.addFromDisk = function(filePath, typeQName)
    {
        var json = JSON.parse("" + fs.readFileSync(filePath));

        if (isArray(json))
        {
            var array = json;

            array.forEach(function(obj) {

                if (typeQName) {
                    obj._type = typeQName;
                }

                r.addNode(obj);
            });
        }
        else if (isObject(json))
        {
            var obj = json;

            if (typeQName) {
                obj._type = typeQName;
            }

            r.addNode(json);
        }
    };

    r.addAttachmentFromDisk = function(docId, attachmentId, filePath)
    {
        r.addAttachment(docId, attachmentId, filePath);
    };

    /**
     * Packages up the contents of the packager into a ZIP file.
     *
     * @param config
     * @param callback
     */
    r.package = function(config, callback)
    {
        if (typeof(config) === "function") {
            callback = config;
            config = {};
        }

        // allow for overrides here
        if (config.archiveGroup) {
            archiveGroup = config.archiveGroup;
        }
        if (config.archiveName) {
            archiveGroup = config.archiveName;
        }
        if (config.archiveVersion) {
            archiveGroup = config.archiveVersion;
        }
        if (config.outputPath) {
            outputPath = config.outputPath;
        }

        // vars
        var skip = config.skip;
        if (!skip) {
            skip = 0;
        }
        var limit = config.limit;
        if (!limit || limit < -1) {
            limit = -1;
        }

        var handleError = function(err) {
            console.log("Caught error during package");
            console.log(err);
            callback(err);
        };

        // applies any automatic record generation
        // this includes generating associations for relator properties
        _automaticRecordGeneration(function(err) {

            // adds container paths into the preparation
            _createContainerPaths(function (err) {

                if (err) {
                    return handleError(err);
                }

                // gather the references within the compiler
                _parseReferences(function(err) {

                    if (err) {
                        return handleError(err);
                    }

                    // call over to cloud cms and request any GUIDs that we need
                    var recordIds = _acquireGUIDs(compiler.count());

                    // resolve all of the IDs for objects in the package
                    _resolveReferences(recordIds, function (err) {

                        if (err) {
                            return handleError(err);
                        }

                        // hook up the attachments to the underlying json objects
                        _bindAttachmentsToObjects(function (err) {

                            if (err) {
                                return handleError(err);
                            }

                            // clean up objects
                            _cleanupObjects(function (err) {

                                if (err) {
                                    return handleError(err);
                                }

                                // verify objects
                                _verifyObjects(function(err) {

                                    if (err) {
                                        return handleError(err);
                                    }

                                    _cleanupOldArchives(archiveGroup, archiveName, archiveVersion, function(err) {

                                        if (err) {
                                            return handleError(err);
                                        }

                                        // figure out how many parts we're going to have
                                        var size = compiler.count();
                                        if (limit > -1)
                                        {
                                            size = limit;
                                        }

                                        report("Total number of records to write: " + size);
                                        var numberOfParts = Math.ceil(size / MAX_RECORDS_PER_ARCHIVE);
                                        report("Writing out: " + numberOfParts + " parts of size: " + MAX_RECORDS_PER_ARCHIVE + " each");
                                        if (numberOfParts === 1)
                                        {
                                            _writeSinglePartArchive(archiveGroup, archiveName, archiveVersion, skip, limit, function (err, archiveFileName) {

                                                if (err) {
                                                    return handleError(err);
                                                }

                                                report("Wrote [group=" + archiveGroup + ", name=" + archiveName + ", version=" + archiveVersion + "]: " + archiveFileName);

                                                callback(null, {
                                                    "group": archiveGroup,
                                                    "name": archiveName,
                                                    "version": archiveVersion,
                                                    "filename": archiveFileName
                                                });
                                            });
                                        }
                                        else
                                        {
                                            _writeMultiPartArchive(numberOfParts, archiveGroup, archiveName, archiveVersion, skip, limit, function (err, archiveFileName) {

                                                if (err) {
                                                    return handleError(err);
                                                }

                                                report("Wrote [group=" + archiveGroup + ", name=" + archiveName + ", version=" + archiveVersion + "]: " + archiveFileName);

                                                callback(null, {
                                                    "group": archiveGroup,
                                                    "name": archiveName,
                                                    "version": archiveVersion,
                                                    "filename": archiveFileName
                                                });
                                            });
                                        }
                                    });

                                });
                            });
                        });
                    });
                });
            });
        });
    };

    var _writeSinglePartArchive = function(group, artifact, version, skip, limit, callback)
    {
        report("Writing single archive");

        var traverser = compiler.traverser(skip, limit);

        // write nodes to temporary zip location on disk
        _commitToDisk(path.join(workingFolder, "package"), group, artifact, version, traverser, function (err) {

            if (err)
            {
                return callback(err);
            }

            // create the archive
            var packageName = (group + "-" + artifact + "-" + version).toLowerCase();
            _createArchiveFile(path.join(workingFolder, "package"), packageName, function (err, archiveFilePath) {

                if (err)
                {
                    return callback(err);
                }

                callback(null, archiveFilePath);
            });
        });

    };

    var _writeMultiPartArchive = function(numberOfParts, group, artifact, version, skip, limit, callback)
    {
        report("Writing multiple archives");

        var start = skip;
        var size = limit;

        if (size === -1 || size > compiler.count()) {
            size = compiler.count();
        }

        var fns = [];
        for (var i = 0; i < numberOfParts; i++)
        {
            var partStart = start + (i * MAX_RECORDS_PER_ARCHIVE);
            var partSize = MAX_RECORDS_PER_ARCHIVE;
            if (partStart + partSize > size) {
                partSize = size - partStart;
            }

            var partTraverser = compiler.traverser(partStart, partSize);

            var fn = function(partTraverser, partIndex) {
                return function(done) {

                    console.log("Writing archive: " + partIndex);

                    // write nodes to temporary zip location on disk
                    _commitToDisk(path.join(workingFolder, 'package-' + partIndex), group, artifact, version, partTraverser, function (err) {

                        if (err)
                        {
                            done(err);
                            return;
                        }

                        // create the archive
                        var packageName = (group + "-" + artifact + "_part" + partIndex + "-" + version).toLowerCase();
                        _createArchiveFile(path.join(workingFolder, 'package-' + partIndex), packageName, function (err, archiveFilePath) {

                            if (err)
                            {
                                done(err);
                                return;
                            }

                            console.log("Completed archive: " + partIndex + " with path: " + archiveFilePath);

                            done(null, archiveFilePath);
                        });
                    });
                }
            }(partTraverser, i);
            fns.push(fn);
        }

        async.series(fns, function(err) {

            if (err) {
                return callback(err);
            }

            // write a single archive that has multi-part dependencies on the others
            var manifest = {};
            manifest["model"] = "2.0.0";
            manifest["group"] = group;
            manifest["artifact"] = artifact;
            manifest["version"] = version;
            manifest["type"] = "package";
            manifest["includes"] = [];
            manifest["parts"] = [];
            //manifest["dependencies"] = [];
            for (var i = 0; i < numberOfParts; i++)
            {
                manifest.parts.push({
                    "group": group,
                    "artifact": artifact + "_part" + i,
                    "version": version
                });
            }

            var manifestFolderPath = path.join(workingFolder, 'package');
            mkdirp.sync(manifestFolderPath);

            report("Writing manifest");

            var StringifyStream = require('stringifystream');
            var Readable = require('stream').Readable;
            var rs = Readable({objectMode: true});
            rs.push(manifest);
            rs.push(null);

            var manifestFileStream = fs.createWriteStream(manifestFolderPath + "/manifest.json");
            manifestFileStream.on('error', function(err) {

                report("An error occurred on commit of objects to disk");

                callback(err);
            });
            manifestFileStream.on('finish', function(){

                report("Completed commit of objects to disk");

                createFinalArchive();
            });

            rs.pipe(StringifyStream()).pipe(manifestFileStream);

            var createFinalArchive = function()
            {
                var packageName = (group + "-" + artifact + "-" + version).toLowerCase();
                _createArchiveFile(path.join(workingFolder, "package"), packageName, function (err, archiveFilePath) {

                    if (err)
                    {
                        callback(err);
                        return;
                    }

                    callback(null, archiveFilePath);
                });
            };

        });
    };

    /**
     * Finds and deletes any existing archive files for the given group, artifact and version.
     * If multipart includes exist, those are cleaned up as well.
     *
     * @param group
     * @param artifact
     * @param version
     * @param callback
     * @private
     */
    var _cleanupOldArchives = function(group, artifact, version, callback) {

        // find any parts if they exist
        var i = 0;
        var partsDone = false;
        do
        {
            var packageFilePath = path.join(basePath, "archives/" + group + "-" + artifact + "_part" + i + "-" + version + ".zip");
            if (fs.existsSync(packageFilePath))
            {
                fs.unlinkSync(packageFilePath);
            }
            else
            {
                partsDone = true;
            }

            i++;
        }
        while (!partsDone);

        var packageFilePath = path.join(basePath, "archives/" + group + "-" + artifact + "-" + version + ".zip");
        if (fs.existsSync(packageFilePath))
        {
            fs.unlinkSync(packageFilePath);
        }

        callback();
    };

    var _walkObject = function(object, fn)
    {
        var walkArray = function(array, propertyPath) {

            var x1 = fn(propertyPath ? propertyPath : "/", array);
            if (x1) {
                array.length = 0;
                Array.prototype.push.apply(array, x1);
            }

            for (var i = 0; i < array.length; i++)
            {
                var val = array[i];

                if (isObject(val))
                {
                    walkObject(val, propertyPath + "[" + i + "]");
                }
                else if (isArray(val))
                {
                    walkArray(val, propertyPath + "[" + i + "]");
                }
                else
                {
                    var x2 = fn(val, propertyPath);
                    if (x2)
                    {
                        array[i] = x2;
                    }
                }
            }
        };

        var walkObject = function(obj, propertyPath) {

            var x1 = fn(propertyPath ? propertyPath : "/", obj);
            if (x1) {

                // clear the existing object
                var props = Object.getOwnPropertyNames(obj);
                for (var i = 0; i < props.length; i++) {
                    delete obj[props[i]];
                }

                // assign into
                Object.assign(obj, x1);
            }

            if (!propertyPath) {
                propertyPath = "";
            }

            // collect keys
            var keys = [];
            for (var k in obj) {
                if (obj.hasOwnProperty(k)) {
                    keys.push(k);
                }
            }

            for (var i = 0; i < keys.length; i++)
            {
                var key = keys[i];
                var val = obj[key];

                if (isObject(val))
                {
                    walkObject(val, propertyPath + "/" + key);
                }
                else if (isArray(val))
                {
                    walkArray(val, propertyPath + "/" + key);
                }
                else
                {
                    var x2 = fn(val, propertyPath);
                    if (x2)
                    {
                        obj[key] = x2;
                    }
                }
            }
        };

        walkObject(object);
    };

    var _findPropertyDefinition = function(definition, instancePropertyKey)
    {
        if (instancePropertyKey.indexOf("/") === 0) {
            instancePropertyKey = instancePropertyKey.substring(1);
        }

        var parts = instancePropertyKey.split("/");

        var d = definition;
        do
        {
            if (parts.length > 0)
            {
                var part = parts.shift();

                var z = part.indexOf("[");
                if (z > -1)
                {
                    if (d.items)
                    {
                        d = d.items;
                    }
                    else
                    {
                        d = null;
                    }
                }
                else
                {
                    if (d.properties)
                    {
                        d = d.properties[part];
                    }
                    else
                    {
                        d = null;
                    }
                }
            }
        }
        while (d && parts.length > 0);

        return d;
    };

    var __doAutoGenerateRelatorAssociationFn = function(record, definitionsMap)
    {
        return function(key, val)
        {
            if (isObject(val))
            {
                var relatedNodeAlias = val["__related_node__"];
                if (relatedNodeAlias)
                {
                    var relatedNodeRecord = compiler.getRecord(relatedNodeAlias);
                    if (relatedNodeRecord)
                    {
                        var associationTypeQName = null;

                        var definition = null;
                        var definitionId = definitionsMap[record.json._type];
                        if (definitionId) {
                            definition = compiler.getObject(definitionId);
                        }
                        if (definition)
                        {
                            var propertyDefinition = _findPropertyDefinition(definition, key);
                            if (propertyDefinition)
                            {
                                if (propertyDefinition._relator && propertyDefinition._relator.associationType)
                                {
                                    associationTypeQName = propertyDefinition._relator.associationType;
                                }
                            }
                        }

                        // assume linked in not found in definition map
                        if (!associationTypeQName)
                        {
                            associationTypeQName = "a:linked";
                        }

                        var associationObject = {
                            "_type": associationTypeQName,
                            "_features": {
                                "f:relator": {
                                    "propertyHolder": "source",
                                    "propertyPath": key
                                }
                            }
                        };

                        _addAssociation(record.id, relatedNodeRecord.id, associationObject);

                        report("Auto-generated relator association from: " + record.id + " to: " + relatedNodeRecord.id + " for path: " + key);
                    }
                }
            }
        };
    };

    var _automaticRecordGeneration = function(callback)
    {
        // collect all of the definitions by qname
        var definitionsMap = {};
        compiler.eachRecord(function(alias, record) {
            if (record.json)
            {
                if (record.json._type === "d:type" || record.json._type === "d:association" || record.json._type === "d:feature")
                {
                    definitionsMap[record.json._qname] = record.id;
                }
            }
        });

        // create folder nodes, etc needed to complete the content graph
        compiler.eachRecord(function(alias, record) {

            var object = record.json;

            _walkObject(object, __doAutoGenerateRelatorAssociationFn(record, definitionsMap));
        });

        callback();
    };

    return r;
};
