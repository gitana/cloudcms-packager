var temp = require("temp");

// Automatically track and cleanup files at exit
temp.track();

module.exports = function() {

    var aliasCounter = 0;

    // gathered up during the "add" phase
    var recordCount = 0;
    var records = {};
    var recordAliases = [];
    var aliasToResolvedIdMap = {};

    // collected at the start of the compile phase
    var references = {};

    // maps alternate ids to record ids
    var alternateIdsMap = {};

    var r = {};

    /**
     * Adds an object to the compiler
     *
     * @param type "node" or "association", other types in the future
     * @param binding a JSON object describing the location of this object, i.e. { "repositoryId": "<repoId>", "branchId": "<branchId>" }
     * @param object the JSON of the object sans _doc field
     * @param alternateIds any array of alternate string based identifiers that we want to retain for lookup later
     *
     * @type {Function}
     */
    var addObject = r.addObject = function(type, binding, object, alternateIds) {

        if (object._doc) {
            throw new Error("Incoming JSON cannot have a predefined _doc field");
        }

        /*
        // TEMP
        if (object.title === "220020446")
        {
            console.log("Adding 220020446");
            console.trace();
        }
        */

        var alias = object._alias;
        if (!alias) {
            alias = "alias-" + (aliasCounter++);
        }

        // stub out the record with a dummy id (alias)
        var record = {
            "id": alias,
            "type": type,
            "attachments": [],
            "json": object,
            "binding": binding
        };

        records[record.id] = record;
        recordCount++;
        recordAliases.push(record.id);

        if (alternateIds) {
            for (var i = 0; i < alternateIds.length; i++) {
                alternateIdsMap[alternateIds[i]] = record.id;
            }
        }

        return record;
    };

    /**
     * Adds an attachment to the compiler payload.  The attachment must be placed onto an existing object.
     *
     * @param alias
     * @param attachmentId
     * @param attachmentSource either a file path or a stream
     *
     * @type {Function}
     */
    var addAttachment = r.addAttachment = function(alias, attachmentId, attachmentSource) {

        // allow the alias to be the record itself, if that's easier
        if (alias.id) {
            alias = alias.id;
        }

        var filePath = null;
        if (typeof(attachmentSource) === "string") {
            filePath = attachmentSource;
        }

        if (!filePath) {
            throw new Error("file path null for alias: " + alias + " and ad: " + attachmentId);
        }

        var record = records[alias];
        if (!record) {
            throw new Error("Cannot find record for alias: " + alias);
        }

        record.attachments.push({
            "id": attachmentId,
            "path": filePath
        });

        return record;
    };

    var getRecord = r.getRecord = function(aliasOrId) {

        // if this alias has already been resolved, then switch to using ID
        if (alternateIdsMap[aliasOrId]) {
            aliasOrId = alternateIdsMap[aliasOrId];
        }

        if (aliasToResolvedIdMap[aliasOrId]) {
            aliasOrId = aliasToResolvedIdMap[aliasOrId];
        }

        return records[aliasOrId];
    };

    var getObject = r.getObject = function(aliasOrId) {

        var obj = null;

        var record = getRecord(aliasOrId);
        if (record)
        {
            obj = record.json;
        }

        return obj;
    };

    var count = r.count = function() {
        return recordCount;
    };

    var getObjects = r.getObjects = function(type) {

        var map = {};

        for (var k in records)
        {
            if (type)
            {
                if (records[k]._type === type)
                {
                    map[k] = records[k].json;
                }
            }
            else
            {
                map[k] = records[k].json;
            }
        }

        return map;
    };

    var getRecords = r.getRecords = function(type) {

        var map = {};

        for (var k in records)
        {
            if (type)
            {
                if (records[k].type === type)
                {
                    map[k] = records[k];
                }
            }
            else
            {
                map[k] = records[k];
            }
        }

        return map;
    };

    var eachRecord = r.eachRecord = function(type, callback) {

        if (typeof(type) === "function") {
            callback = type;
            type = null;
        }

        var map = getRecords(type);

        for (var i = 0; i < recordAliases.length; i++)
        {
            var id = recordAliases[i];

            var record = map[id];
            if (!record) {
                var resolvedId = aliasToResolvedIdMap[id];
                if (resolvedId)
                {
                    id = resolvedId;
                    record = map[id];
                }
            }
            if (record) {
                callback(id, record);
            }
        }

    };

    /**
     * Marks that an object described by an alias has another object (described by dependentAlias) that depends on it.
     *
     * @type {Function}
     */
    var addReference = r.addReference = function(alias, aliasOfSomeoneWhoWeReference)
    {
        if (!references[alias]) {
            references[alias] = [];
        }

        references[alias].push(aliasOfSomeoneWhoWeReference);
    };

    /**
     * Marks that an alias has been resolved to a GUID.
     *
     * @type {Function}
     */
    var resolveReference = r.resolveReference = function(alias, resolvedId)
    {
        // find original record
        var record = getRecord(alias);
        if (!record)
        {
            throw new Error("Cannot resolve reference for alias: " + alias);
        }

        aliasToResolvedIdMap[alias] = resolvedId;

        record._alias = record.id;
        record.id = resolvedId;
        record.json._doc = resolvedId;

        delete records[alias];
        records[resolvedId] = record;
    };

    /**
     * Returns a list of alias IDs that reference us.  Given an alias ID, hands back the alias IDs of other objects
     * that have at least one member variable pointing to us.
     *
     * @type {Function}
     */
    var getReferences = r.getReferences = function(alias)
    {
        var refs = [];

        if (references[alias])
        {
            refs = references[alias];
        }

        return refs;
    };

    var assertComplete = r.assertComplete = function(callback)
    {
        // size of resolvedAliases should be the same as the total count
        var resolvedAliasesSize = 0;
        for (var k in aliasToResolvedIdMap) {
            resolvedAliasesSize++;
        }
        if (count() !== resolvedAliasesSize)
        {
            callback({
                "message": "Resolved aliases size is: " + resolvedAliasesSize + " and it should be: " + count()
            });
            return;
        }

        callback();
    };

    var getResolvedIdForAlias = r.getResolvedIdForAlias = function(alias)
    {
        return aliasToResolvedIdMap[alias];
    };

    var traverser = r.traverser = function(_skip, _limit)
    {
        var skipValue = 0;
        var limitValue = -1;

        if (typeof(_skip) !== "undefined") {
            if (_skip < recordCount) {
                skipValue = _skip;
            }
        }

        if (typeof(_limit) !== "undefined") {
            limitValue = _limit;
        }

        var t = {};

        var eachRecord = t.eachRecord = function(fn) {
            var c = 0;
            var maxCount = recordCount;
            if (limitValue !== -1) {
                maxCount = skipValue + limitValue;
            }
            if (maxCount > recordCount) {
                maxCount = recordCount;
            }
            var records = getRecords();
            for (var k in records)
            {
                //console.log("c: " + c + ", skip: " + skipValue + ", max: " + maxCount);

                if (c >= skipValue && c < maxCount)
                {
                    fn(k, records[k], c, maxCount);
                    //console.log("FB");
                }

                c++;
            }
        };

        var eachObject = t.eachObject = function(fn) {
            eachRecord(function(k, r, c, m) {
                fn(k, r.json, c, m);
            });
        };

        return t;
    };

    return r;
};
