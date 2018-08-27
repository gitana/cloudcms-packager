// create a packager
//var PackagerFactory = require("cloudcms-packager");
var PackagerFactory = require("../../../../index.js");
PackagerFactory.create({
    "outputPath": "archives",
    "archiveGroup": "worldcup2014squads",
    "archiveName": "example3"
}, function(err, packager) {

    // err if the packager failed to init
    if (err) {
        return console.error(err);
    }

    // package up content definitions
    packager.addFromDisk("./types/my_athlete/node.json");
    packager.addFromDisk("./types/my_club/node.json");
    packager.addFromDisk("./types/my_team/node.json");

    // load in content from JSON
    var array = require("./data/2014-world-cup-squads.json");
    // parse into team > club > athlete
    var teamMap = {};
    var teamCount = 0;
    var clubMap = {};
    var clubCount = 0;
    for (var i = 0; i < array.length; i++)
    {
        var entry = array[i];

        var alias = "athlete" + i;

        var TeamObject = teamMap[entry.Team];
        if (!TeamObject) {
            TeamObject = teamMap[entry.Team] = {
                "title": entry.Team,
                "club": null,
                "_type": "my:team",
                "_alias": "team" + (teamCount++)
            };

            // add team to packager
            packager.addNode(TeamObject);
        }

        var ClubObject = clubMap[entry.Club];
        if (!ClubObject) {
            ClubObject = clubMap[entry.Club] = {
                "title": entry.Club,
                "country": entry.ClubCountry,
                "athletes": [],
                "_type": "my:club",
                "_alias": "club" + (clubCount++)
            };

            // add club to packager
            packager.addNode(ClubObject);
        }

        if (!TeamObject.club) {
            TeamObject.club = {
                "__related_node__": ClubObject._alias
            };
        }

        var AthleteObject = {
            "title": entry.FullName,
            "number": entry.Number,
            "position": entry.Position,
            "dateOfBirth": entry.DateOfBirth,
            "isCaptain": entry.isCaptain,
            "_type": "my:athlete",
            "_alias": alias
        };

        // link club to athlete
        ClubObject.athletes.push({
            "__related_node__": AthleteObject._alias
        });

        // add athlete to packager
        packager.addNode(AthleteObject);
    }

    // commit
    packager.package(function(err, archiveInfo) {

        // err if the packager failed to commit
        if (err) {
            return console.error(err);
        }

    });

});

