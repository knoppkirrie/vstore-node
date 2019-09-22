var fs = require('fs');
var Thumbnail = require('thumbnail');
var tmp_thumb_dir = './tmp_thumb';
const md5File = require('md5-file');

// ----- REPLICATION vars and consts below -----
var tmp_replication_dir = "./tmp_replication";

var MASTER_NODE_URL = "http://ec2-35-156-157-137.eu-central-1.compute.amazonaws.com";
var MASTER_NODE_PORT = 50000;
var MASTER_API_VERSION = "v1";

var nGeohash = require('ngeohash');
const GEOHASH_COMPARE_PRECISION = 5;
const GEOHASH_PRECISION = 9;    // highest possible: 9

const REPLICATION_COUNTER_THRESHOLD = 5;

const REPLICATION_RETAINING_THRESHOLD = 7;  // in days

var cron = require('node-cron');
var request = require('request');

// LOGGING
const eventTypes = {
    RECEIVE_UPLOAD : "RECEIVE_UPLOAD",
    RECEIVE_REPLICATION : "RECEIVE_REPLICATION",
    SERVE_ORIGINAL_FILE : "SERVE_ORIGINAL_FILE",
    SERVE_REPLICATION : "SERVE_REPLICATION",
    ALREADY_REPLICATED : "ALREAY_REPLICATED",
    RECEIVE_FILEACCESS : "RECEIVE_FILEACCESS",
    RESET_FILEACCESSLOCATION : "RESET_FILEACCESSLOCATION",
    REPLICATION_CRONJOB : "REPLICATION_CRONJOB",
    RETAINING_CRONJOB : "RETAINING_CRONJOB",
    DELETE_REPLICATION : "DELETE_REPLICATION",
    TRIGGER_REPLICATION : "TRIGGER_REPLICATION",
    SAVED_REPLICATION : "SAVED_REPLICATION",
    REPLICATION_ERROR : "REPLICATION_ERROR",
    NODE_CONNECTED : "NODE_CONNECTED"
}

if(!fs.existsSync("./log")) {
    fs.mkdirSync("./log");
}
//const LOG_FILE = "./log/" + (new Date().toISOString().replace(/:/g, "-") + ".csv");
const LOG_FILE = "../node_logging.csv";

var logStream = fs.createWriteStream(LOG_FILE, {flags:'a'});
logStream.on("open", function(fd) {
    //logStream.write("Timestamp, EventType, vStore-UUID, Obj_id, targetNode\n");
    logStream.write(new Date().toISOString() + ", , " + eventTypes.NODE_CONNECTED + ", , , , \n");
});

// ----- REPLICATION vars and consts above -----

if(!fs.existsSync(tmp_thumb_dir)) {
    fs.mkdirSync(tmp_thumb_dir);
}

// create tmp_replication folder if not exists
if (!fs.existsSync(tmp_replication_dir)){
    fs.mkdirSync(tmp_replication_dir);
}

var image_types = ["image/jpeg", "image/png", "image/gif", "image/bmp"];
var video_types = ["video/mp4", "video/mov", "video/3gpp"];
var document_types = ["application/msword", "application/msexcel", "application/pdf", "application/txt"];
var contact_types = ["text/vcard"];
var audio_types = ["audio/mpeg", "audio/mp4", "audio/wav", "audio/aac"];

function getDateTime() {
    return (new Date()).toJSON().slice(0, 19).replace(/[-T]/g, ':');
}

module.exports = function(app, upload, mongoose, dbConn, NODE_UUID, NODE_TYPE, NODE_PORT, ffmpeg)
{
    var m = require('./models.js')(mongoose);
    var Grid = require('gridfs-stream');
    Grid.mongo = mongoose.mongo;
    var gfs = Grid(dbConn.db);

    //******************//
    //*** API routes ***//
    //******************//

    // Route for requesting the UUID of this node
    app.get('/uuid', function(req, res)
    {
        console.log("Received an id request");
        res.setHeader('Content-Type', 'application/json');
        res.json({'uuid':NODE_UUID, 'type':NODE_TYPE});
    });

    // Route for uploading a new file
    app.post('/file/data', upload.single('filedata'), function(req, res, next)
    {
        console.log("["+getDateTime()+"] Upload request received for new file");

        //Check if all fields are available. If not, reply with error.
        if(!req.file || !req.body || !req.body.descriptiveName || !req.file.originalname
           || !req.body.context || !req.body.mimetype || !req.body.extension
           || !req.body.isPrivate || !req.body.phoneID)
        {
            console.log("["+getDateTime()+"] Invalid upload request received");
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        //Get field values from the request
        var context = parseCtxToModel(req.body.context);
        var uuid = req.file.originalname.replace(/\.[^/.]+$/, "");
        var originalname =  req.body.descriptiveName;
        var mimetype = req.body.mimetype;
        var extension = req.body.extension;
        var filesize = req.body.filesize;
        var creationdate = ((typeof req.body.creationdate !== undefined)
                            ? req.body.creationdate
                            : new Date().getTime());
        var isPrivate = ((req.body.isPrivate.indexOf("true") !== -1) ? true : false);
        var phoneID = req.body.phoneID;

        console.log("        UUID: " + uuid);

        fileLog(eventTypes.RECEIVE_UPLOAD, uuid, "", "");

        //Compute MD5 hash of uploaded file synchronously and check if
        //this file is already in the database
        const hash = md5File.sync(req.file.path);

        //Populate a new file model with the metadata
        var fileModel = new m.File(
        {
            "uuid" : uuid,
            "md5": hash,
            "descriptiveName" : originalname,
            "mimetype" : mimetype,
            "extension": extension,
            "filesize" : filesize,
            "creationTimestamp" : creationdate,
            "context" : context,
            "isPrivate" : isPrivate,
            "phoneID" : phoneID
        });

        //Check if file with the given hash is already contained in the database.
        m.File.find({ "md5" : hash }, function(err, result)
        {
            if(!result || result.length != 0)
            {
                //Reply with error that file already exists.
                res.status(409).json({'error':1, 'error_msg':"The file already exists."});
                return;
            }

            //First, write to GridFS, then create thumbnail,
            //then save metadata in database
            var writestream = gfs.createWriteStream(
            {
                filename: uuid
            });
            fs.createReadStream(req.file.path).pipe(writestream);
            var thumbWriteStream = gfs.createWriteStream(
            {
                filename: 'thumb_'+uuid
            });

            //Create a thumbnail according to the filetype.
            //Needs improvement.
            if(image_types.includes(mimetype))
            {
                //Rename file to have file extension so that
                //thumbnail module will accept it
                fs.renameSync(req.file.path, req.file.path+"."+extension);
                //Create thumbnail and store in GridFS as well
                var original_path = req.file.destination;
                var thumbnail = new Thumbnail(original_path, tmp_thumb_dir);
                thumbnail.ensureThumbnail(req.file.filename+"."+extension, 256, null, function(err, createdThumbName)
                {
                    if(err) { console.log("Error creating image thumbnail. Details: " + err); return; }
                    fs.createReadStream(tmp_thumb_dir + '/' + createdThumbName).pipe(thumbWriteStream);
                    thumbWriteStream.on('close', function(file)
                    {
                        //Store meta information in document
                        fileModel.save(function(err){});
                        res.status(201).json({'error':0, 'reply':'File stored successfully.'});
                        //Delete both tempfiles (thumb and original file)
                        fs.unlinkSync(tmp_thumb_dir + '/' + createdThumbName);
                        fs.unlinkSync(req.file.path+"."+extension);
                    });
                });
            }
            else if(video_types.includes(mimetype))
            {
                var proc = new ffmpeg(req.file.path).thumbnail(
                {
                    count: 1,
                    timemarks: ['0'],
                    folder: tmp_thumb_dir,
                    filename: 'thumb_'+uuid,
                    size: '256x?'
                })
                .on('end', function(stdout, stderr) {
                    fs.createReadStream(tmp_thumb_dir + '/thumb_'+uuid+'.png')
                        .pipe(thumbWriteStream);
                    thumbWriteStream.on('close', function(file)
                    {
                        //Store meta information in document
                        fileModel.save(function(err){});
                        res.status(201).json({'error':0, 'reply':'File stored successfully.'});
                        //Delete both tempfiles (thumb and original file)
                        fs.unlinkSync(tmp_thumb_dir + '/thumb_'+uuid+'.png');
                        fs.unlinkSync(req.file.path);
                    });
                });
            }
            else if(contact_types.includes(mimetype))
            {
                fs.createReadStream('./icons/ic_contact.png').pipe(thumbWriteStream);
                thumbWriteStream.on('close', function(file)
                {
                    //Store meta information in document
                    fileModel.save(function(err){});
                    res.status(201).json({'error':0, 'reply':'File stored successfully.'});
                    //Delete original file
                    fs.unlinkSync(req.file.path);
                });
            }
            else if(audio_types.includes(mimetype))
            {
                fs.createReadStream('./icons/ic_audio.png').pipe(thumbWriteStream);
                thumbWriteStream.on('close', function(file)
                {
                    //Store meta information in document
                    fileModel.save(function(err){});
                    res.status(201).json({'error':0, 'reply':'File stored successfully.'});
                    //Delete original file
                    fs.unlinkSync(req.file.path);
                });
            }
            else
            {
                fs.createReadStream('./icons/ic_unknown_file.png').pipe(thumbWriteStream);
                thumbWriteStream.on('close', function(file)
                {
                    //Store meta information in document
                    fileModel.save(function(err){});
                    res.status(201).json({'error':0, 'reply':'File stored successfully.'});
                    //Delete original file
                    fs.unlinkSync(req.file.path);
                });
            }
        });
    });

    // Route for getting a thumbnail by uuid
    // The thumbnail will only be provided if the requested file is not
    // marked as private. If it is marked private, it will only be provided if
    // the requesting device ID matches the file creator's device id in the database.
    app.get('/thumbnail/:uuid/:phoneID', function(req, res)
    {
        //Check if necessary parameters are given. If not, reply with error.
        if(!req.params || !req.params.uuid || !req.params.phoneID)
        {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var fUUID = req.params.uuid;
        var phoneID = req.params.phoneID

        console.log("["+getDateTime()+"] Thumbnail request received for " + fUUID);

        //Read metadata for file from database and check if file is not private.
        //If file is private, check if the correct phone id is requesting the file
        m.File.findOne({'uuid': req.params.uuid}, function(err, result)
        {
            //Check if error occurred
            if(err)
            {
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                console.log(err);
                return;
            }

            //Check if result is not valid
            if(!result)
            {
                res.status(404).json({'error':1, 'error_msg':"Thumbnail not found."});
                return;
            }

            //Check if user is allowed to read thumbnail
            if(result.isPrivate && (phoneID != result.phoneID))
            {
                res.status(404).json({'error':1, 'error_msg':"Thumbnail not found!"});
                return;
            }

            //Read thumbnail from GridFS
            gfs.exist({filename: 'thumb_'+fUUID}, function(err, found)
            {
                if(!found)
                {
                    res.status(404).json({'error':1, 'error_msg':"Thumbnail not found."});
                    return;
                }

                var readstream = gfs.createReadStream({filename: 'thumb_'+fUUID});
                //Set response header to the corresponding mime type
                res.setHeader('Content-Type', "image/jpeg");
                //Allow caching of thumbnails for 24 hours
                res.setHeader('Cache-Control', "max-age=86400");

                try
                {
                    readstream.pipe(res);
                }
                catch(err)
                {
                    console.log(err);
                    res.status(500).json({'error':1, 'error_msg':"Failed to write stream!"});
                    return;
                }
            });
        });
    });

    // Route for requesting metadata information about the file
    // The metadata will only be provided if the requested file is not
    // marked as private. If it is marked private, the metadata will only be
    // provided if the requesting device ID matches the file creator's device
    // id in the database.
    app.get('/file/metadata/full/:uuid/:phoneID', function(req, res)
    {
        if(!req.params || !req.params.uuid || !req.params.phoneID)
        {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var fUUID = req.params.uuid;
        var phoneID = req.params.phoneID;
        m.File.findOne({'uuid': req.params.uuid}, function(err, result)
        {
            if(err)
            {
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                console.log(err);
                return;
            }

            if(!result)
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            if(result.isPrivate && (phoneID != result.phoneID))
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            //Delete phone id from creator from the result
            delete result.phoneID;
            //Read MetaData from GridFS (to determine filesize)
            gfs.findOne({filename: fUUID}, function(err, metadata) {
                if(!err && metadata)
                {
                    res.status(200).json(
                    {
                        'error':0,
                        'reply':
                        {
                            'metadata':result,
                            'filesize': metadata.length //in bytes
                        }
                    });
                }
                else
                {
                    res.status(404).json({'error':1, 'reply':"File not found."});
                }
            });
        });
    });


    // Route for requesting only lightweight metadata information about the file.
    // This means: Without the context information.
    // The metadata will only be provided if the requested file is not
    // marked as private. If it is marked private, the metadata will only be
    // provided if the requesting device ID matches the file creator's device
    app.get('/file/metadata/light/:uuid/:phoneID', function(req, res)
    {
        if(!req.params || !req.params.uuid || !req.params.phoneID)
        {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var fUUID = req.params.uuid;
        var phoneID = req.params.phoneID;
        m.File.findOne({'uuid': req.params.uuid}, function(err, result)
        {
            if(err)
            {
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                console.log(err);
                return;
            }

            if(!result)
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            if(result.isPrivate && (phoneID != result.phoneID))
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            //Delete phone id and context from creator from the result
            delete result.phoneID;
            delete result.context;

            //Read MetaData from GridFS (to determine filesize)
            gfs.findOne({filename: fUUID}, function(err, metadata) {
                if(!err && metadata)
                {
                    res.status(200).json(
                    {
                        'error':0,
                        'reply':
                        {
                            'metadata':result,
                            'filesize': metadata.length //in bytes
                        }
                    });
                }
                else
                {
                    res.status(404).json({'error':1, 'reply':"File not found."});
                }
            });
        });
    });


    // Route for downloading the full file by uuid
    // The file will only be provided if it is not marked as private. If it
    // is marked private, it will only be provided if the requesting device ID
    // matches the file creator's device id in the database.
    app.get('/file/data/:uuid/:phoneID', function(req, res)
    {
        if(!req.params || !req.params.uuid || !req.params.phoneID)
        {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var fUUID = req.params.uuid;
        var phoneID = req.params.phoneID;

        console.log("["+getDateTime()+"] Download request received for file " + fUUID);

        m.File.findOne({'uuid': fUUID}, function(err, result)
        {
            if(err)
            {
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                console.log(err);
                return;
            }

            if(!result)
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            if(result.isPrivate && (phoneID != result.phoneID))
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            gfs.exist({filename: req.params.uuid}, function(err, found)
            {
                if(!found)
                {
                    //File not found in GridFS, so we delete it from the file collection
                    File.delete({'uuid':fUUID});
                    res.status(404).json({'error':1, 'reply':"File not found."});
                    return;
                }

                var filename = result.uuid + '.' + result.extension;
                var readstream = gfs.createReadStream({filename: req.params.uuid});
                //Set response header to the corresponding mime type
                res.set('Content-Type', result.mimetype);
                res.set('Content-Disposition', 'attachment; filename="'+filename+'"');
                //Allow caching of file for 24 hours
                res.setHeader('Cache-Control', "max-age=86400");
                //Handle deletion while reading using try/catch
                readstream.on('error', function(err) {
                    res.status(404).end();
                    return;
                });
                readstream.pipe(res);

                // if file is a replicated copy, update lastAccess timestamp
                m.ReplicatedFile.findByIdAndUpdate(fUUID, {"lastAccess": Date.now()}, function(err, result) {
                    if (result) {
                        fileLog(eventTypes.SERVE_REPLICATION, fUUID, "", "");
                    } else {
                        fileLog(eventTypes.SERVE_ORIGINAL_FILE, fUUID, "", "");
                    }
                 });

            });
        });
    });

    // Route for getting the mime type of the file with the provided UUID
    app.get('/file/mimetype/:uuid/:phoneID', function(req, res)
    {
        if(!req.params || !req.params.uuid || !req.params.phoneID)
        {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var fUUID = req.params.uuid;
        var phoneID = req.params.phoneID;

        console.log("["+getDateTime()+"] Mimetype request received for " + fUUID);

        m.File.findOne({'uuid': fUUID}, function(err, result)
        {
            if(err)
            {
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                console.log(err);
                return;
            }

            if(!result)
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            if(result.isPrivate && (phoneID != result.phoneID))
            {
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            res.status(200).json({'error':0, 'reply': {'mimetype':result.mimetype}});
        });
    });

    // Route for getting a file list based on the context passed as parameter
    // Empty upload array as placeholder for multer
    app.post('/file/search', upload.array(), function(req, res, next)
    {
        console.log("["+getDateTime()+"] New search request received");

        if(!req.body || !req.body.context)
        {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }
        var rawCtx;
        try {
            rawCtx = JSON.parse(req.body.context);
        }
        catch(err)
        {
            console.log(err);
            res.status(400).json({'error':1, 'error_msg':"Malformed request 2."});
            return;
        }

        // Parse context into mongoose context model
        var context = parseCtxToModel(req.body.context);

        //Build the query based on context filter from request
        var query = {};
        if(context.location)
        {
            if(rawCtx.radius)
            {
                //Limit to 5km max
                if(rawCtx.radius > 5000)
                {
                    rawCtx.radius = 2000;
                }
            }
            else
            {
                rawCtx.radius = 250;
            }
            query['context.location.loc'] =
            {
                $near:
                {
                    $geometry:
                    {
                        type: "Point",
                        coordinates: context.location.loc
                    },
                    $maxDistance: rawCtx.radius
                }
             };
        }
        if(context.place)
        {
            query['context.places.places.name'] = context.place;
        }
        if(context.activity)
        {
            query['context.activity'] = context.activity;
        }
        if(context.network)
        {
            query['context.network'] = context.network;
        }
        if(context.noise)
        {
            query['context.noise.isSilent'] = context.noise.isSilent;
        }
        if(context.weekday)
        {
            query['context.weekday'] = context.weekday;
        }
        if(rawCtx.timeOfDay && rawCtx.timeSpanMS && context.time)
        {
            //Timespan of files must be timespan before and after
            var timeBefore = new Date(context.time.getTime() - rawCtx.timeSpanMS);
            var timeAfter  = new Date(context.time.getTime() + rawCtx.timeSpanMS);
            query['context.hours'] = {$gte: timeBefore.getHours(), $lte: timeAfter.getHours()};
        }

        //Do not start to query the database if no filter is provided,
        //because we cannot provide all files for a request.
        if(Object.keys(query).length == 0)
        {
            var reply = {"files" : []};
            res.status(200).json({'error':0, 'reply':reply});
            return;
        }
        query.isPrivate = false;

        //Fetch matching files from database and reply with their UUIDs
        var files = [];
        m.File.find(query, function(err, results)
        {
            if(err)
            {
                console.log(err);
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                console.log(err);
                return;
            }

            if(!results)
            {
                var reply = {"files" : []};
                res.status(200).json({'error':0, 'reply':reply});
            }

            results.forEach(function(doc)
            {
                files.push(
                {
                    "uuid":doc.uuid,
                    "creationTimestamp":doc.creationTimestamp,
                    "descriptiveName" : doc.descriptiveName,
                    "mimetype":doc.mimetype,
                    "filesize" : doc.filesize
                });
            });

            var reply = {"files" : files};
            res.status(200).json({'error':0, 'reply':reply});
        });
    });


    //Route for deleting a file. Only the creator of the file can issue
    //a delete request by providing his phone ID.
    app.delete('/file/:uuid/:phoneID', function(req, res)
    {
        if(!req.params || !req.params.uuid || !req.params.phoneID)
        {
            console.log("        Malformed request.");
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var fUUID = req.params.uuid;
        var phoneID = req.params.phoneID

        console.log("["+getDateTime()+"] Delete request received for " + fUUID);

        //Read metadata for file from database and check if file is not private.
        //Only allow deletion of the file, if the correct user who uploaded
        //the file sent the request
        m.File.findOne({uuid : fUUID, phoneID : phoneID}, function(err, result)
        {
            if(err)
            {
                console.log("        Error while accessing MongoDB.");
                res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                return;
            }

            if(!result)
            {
                console.log("        No file entry found in MongoDB.");
                res.status(404).json({'error':1, 'reply':"File not found."});
                return;
            }

            //Remove the document from MongoDB
            result.remove();

            //Check if the file exists in GridFS
            gfs.exist({filename: 'thumb_'+fUUID}, function(err, found)
            {
                if(!found)
                {
                    console.log("        File not found in GridFS.");
                    res.status(404).json({'error':1, 'error_msg':"File not found!"});
                    return;
                }

                //Remove the file from GridFS
                gfs.remove({filename: fUUID}, function(err)
                {
                    if(err)
                    {
                        console.log("        Error removing it from GridFS.");
                        res.status(500).json({'error':1, 'error_msg':"Internal server error."});
                        return;
                    }
                    res.status(200).json({'error':0, 'reply':"File successfully deleted."});
                    //Remove thumbnail from GridFS
                    gfs.remove({filename: 'thumb_'+fUUID}, function(err) {});
                });
            });
        });
    });


    //*************************//
    //*** REPLICATION BELOW ***//
    //*************************//

    app.post('/replication/data', upload.single('filedata'), function(req, res) {
        
        console.log('['+getDateTime()+'] Replication request received');

        var original = req.file.originalname;
        var uuid = req.file.originalname.replace(/\.[^/.]+$/, "");

        var metadata = JSON.parse(req.body.metadata);
        delete metadata._id;
        delete metadata.__v;

        var fileModel = new m.File(metadata);

        fileLog(eventTypes.RECEIVE_REPLICATION, uuid, "", "");

        // check if file is already present:
        m.File.findOne({uuid: metadata.uuid}, function(error, file) {
            if (error) {
                console.log('['+getDateTime()+'] File lookup failed!');

                res.status(500).json({"error": 1, "error_msg": "Failed to replicate file!"})
                return;
            }

            if (file) {
                console.log('['+getDateTime()+'] File already present on this node.');
                res.status(200).json({"error": 0, "msg": "Replication succeeded: File is already here."});

                fileLog(eventTypes.ALREADY_REPLICATED, uuid, "", "");

                return;
            }

            var mimetype = metadata.mimetype;
            var extension = metadata.extension;

            // COPY FROM app.post('/file/data', ...) :
            //First, write to GridFS, then create thumbnail,
            //then save metadata in database
            var writestream = gfs.createWriteStream(
            {
                filename: uuid
            });
            fs.createReadStream(req.file.path).pipe(writestream);
            var thumbWriteStream = gfs.createWriteStream(
            {
                filename: 'thumb_'+uuid
            });

            //Create a thumbnail according to the filetype.
            //Needs improvement.
            if(image_types.includes(mimetype))
            {
                //Rename file to have file extension so that
                //thumbnail module will accept it
                fs.renameSync(req.file.path, req.file.path+"."+extension);
                //Create thumbnail and store in GridFS as well
                var original_path = req.file.destination;
                var thumbnail = new Thumbnail(original_path, tmp_thumb_dir);
                thumbnail.ensureThumbnail(req.file.filename+"."+extension, 256, null, function(err, createdThumbName)
                {
                    if(err) { console.log("Error creating image thumbnail. Details: " + err); return; }
                    fs.createReadStream(tmp_thumb_dir + '/' + createdThumbName).pipe(thumbWriteStream);
                    thumbWriteStream.on('close', function(file)
                    {
                        //Store meta information in document
                        fileModel.save(function(err){});
                        res.status(201).json({'error':0, 'reply':'Replication succeeded: File stored successfully.'});
                        //Delete both tempfiles (thumb and original file)
                        fs.unlinkSync(tmp_thumb_dir + '/' + createdThumbName);
                        fs.unlinkSync(req.file.path+"."+extension);

                        // inform master node about new file on this node
                        updateMappingOnMasterNode(fileModel);
                    });
                });
            }
            else if(video_types.includes(mimetype))
            {
                var proc = new ffmpeg(req.file.path).thumbnail(
                {
                    count: 1,
                    timemarks: ['0'],
                    folder: tmp_thumb_dir,
                    filename: 'thumb_'+uuid,
                    size: '256x?'
                })
                .on('end', function(stdout, stderr) {
                    fs.createReadStream(tmp_thumb_dir + '/thumb_'+uuid+'.png')
                        .pipe(thumbWriteStream);
                    thumbWriteStream.on('close', function(file)
                    {
                        //Store meta information in document
                        fileModel.save(function(err){});
                        res.status(201).json({'error':0, 'reply':'Replication succeeded: File stored successfully.'});
                        //Delete both tempfiles (thumb and original file)
                        fs.unlinkSync(tmp_thumb_dir + '/thumb_'+uuid+'.png');
                        fs.unlinkSync(req.file.path);

                        // inform master node about new file on this node
                        updateMappingOnMasterNode(fileModel);
                    });
                });
            }
            else if(contact_types.includes(mimetype))
            {
                fs.createReadStream('./icons/ic_contact.png').pipe(thumbWriteStream);
                thumbWriteStream.on('close', function(file)
                {
                    //Store meta information in document
                    fileModel.save(function(err){});
                    res.status(201).json({'error':0, 'reply':'Replication succeeded: File stored successfully.'});
                    //Delete original file
                    fs.unlinkSync(req.file.path);

                    // inform master node about new file on this node
                    updateMappingOnMasterNode(fileModel);
                });
            }
            else if(audio_types.includes(mimetype))
            {
                fs.createReadStream('./icons/ic_audio.png').pipe(thumbWriteStream);
                thumbWriteStream.on('close', function(file)
                {
                    //Store meta information in document
                    fileModel.save(function(err){});
                    res.status(201).json({'error':0, 'reply':'Replication succeeded: File stored successfully.'});
                    //Delete original file
                    fs.unlinkSync(req.file.path);

                    // inform master node about new file on this node
                    updateMappingOnMasterNode(fileModel);
                });
            }
            else
            {
                fs.createReadStream('./icons/ic_unknown_file.png').pipe(thumbWriteStream);
                thumbWriteStream.on('close', function(file)
                {
                    //Store meta information in document
                    fileModel.save(function(err){});
                    res.status(201).json({'error':0, 'reply':'Replication succeeded: File stored successfully.'});
                    //Delete original file
                    fs.unlinkSync(req.file.path);

                    // inform master node about new file on this node
                    updateMappingOnMasterNode(fileModel);
                });
            }

            // make note that file is a replicated one
            var replicatedFileData = {
                _id: fileModel.uuid,
                lastAccess: Date.now(),
                geohash_prefix: req.body.geohash_prefix,
                src_address: req.connection.remoteAddress,
                src_port: req.body.src_port
            };
            var replicatedFile = new m.ReplicatedFile(replicatedFileData);
            
            replicatedFile.markModified('object');
            
            replicatedFile.save(function(err){

                if (err) {
                    console.log('['+getDateTime()+'] Error saving ReplicatedFile!');
                    console.log('       ' + err);

                    fileLog(eventTypes.REPLICATION_ERROR, replicatedFile._id, replicatedFile.src_address, replicatedFile.src_port);

                    return;
                }

                fileLog(eventTypes.SAVED_REPLICATION, replicatedFile._id, replicatedFile.src_address, replicatedFile.src_port);

            });

        });      

    });


    app.post('/fileAccess/insert', function(req, res)
    {
        console.log('['+getDateTime()+'] FileAccess request received');

        if ( !req.body || !req.body.fileAccesses)
        {
            console.log("["+getDateTime()+"] FileAccess request is invalid.");
            console.log(req.body);
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        var array = req.body.fileAccesses;
       
        var fileGeohashSet = new Set();     // stores keys for later retrieval of FileAccessLocations from node-DB

        var counter = 0;    

        for (var i in array) {
            var uuid = array[i].uuid;
            var file = array[i].file;
            var geohash = array[i].geohash;
            var timestamp = array[i].timestamp;
            var deviceId = array[i].deviceId;

            fileLog(eventTypes.RECEIVE_FILEACCESS, "", uuid, "");

            // combine fileUuid and geohash substring, according to GEOHASH_COMPARE_PRECISION 
            var fileHashPair = file + '###' + geohash.substring(0, GEOHASH_COMPARE_PRECISION);   // separate by '###' to bypass object equality check in Set

            fileGeohashSet.add(fileHashPair); 

            var fa = new m.FileAccess(
            {
                'uuid' : uuid,
                'file': file,
                'geohash': geohash,
                'timestamp': timestamp,
                'deviceId': deviceId
            });

            counter++;
                
            fa.save(function(err){

                counter--;

                // calculate new MeanAccessLocations after all FileAccess objs have been inserted
                if (counter == 0) {
                    calculateMeanAccessLocations( fileGeohashSet );
                }

                // if (err) {
                //     console.log("Error occurred:");
                //     console.log("uuid: " + uuid);
                //     console.log("file: " + file);
                //     console.log("geohash: " + geohash);
                //     console.log("tow: " + timeOfWeek);
                //     console.log("totalMinutes: " + totalMinutes);
                // }

            });
        }
        
        res.status(200).json({'error':0, 'reply':'FileAccess objs added to node db.'});

    });


    /**
     * Is called when a remote node decides to delete a replicated file to free storage.
     * Resets the counter and isReplicated flag for the FileAccessLocation on this node.
     */
    app.post('/replication/reset', function(req, res) {

        if (!req.body.file || !req.body.geohash_prefix) {
            res.status(400).json({'error':1, 'error_msg':"Malformed request."});
            return;
        }

        fileLog(eventTypes.RESET_FILEACCESSLOCATION, req.body.file, req.body.geohash_prefix, req.body.geohash_prefix.length);
        

        // reset all FileAccessLocations whose replication node was the remote one
        //for (var i = 0; i < req.body.geohash_prefix.length; i++) {
        for (var prefix of req.body.geohash_prefix) {

            m.FileAccessLocation.updateMany({file: req.body.file, geohash: new RegExp('^' + prefix)}, {$set: {counter: 0, replicated: false}}, function(err) {
                if (err) {
                    console.log("["+getDateTime()+"] Error resetting FileAccessLocations in MongoDB.");
                    res.status(500).json({'error':1, 'error_msg':"Internal server error."});

                    fileLog("RESET_ERROR", req.body.file, prefix, "");

                    return;
                }
    
                fileLog("RESET_SUCCESS", req.body.file, prefix, "");
                
            });

        }

        res.status(200).json({'error':0, 'msg':"Replication reset."});
        
    });

    /**
     * Calculates new center points for spatially close FileAccess entries
     * @param {*} keySet Set holding "<filename>###<geohash>" Strings to retrieve similar FileAccess objects
     */
    function calculateMeanAccessLocations( keySet ) {
        
        // for all file/geohash pairs in accessedFiles:
        // - get existing FileAccessLocations with pairs from keySet from node-DB

        keySet.forEach(function(fileHashPair) {

            var pair = fileHashPair.split('###');
            var file = pair[0];
            var geohash = pair[1];

            var query = {file: file, geohash: new RegExp('^' + geohash)};   // regex is similar to '<geohash>%' in SQL

            m.FileAccess.find(query, function(err, result) 
            {
                if (err) {
                    console.log("[" + getDateTime() + "] Error retrieving FileAccess objs from MongoDB:")
                    console.log(err)
                    return
                }

                if (!result) {
                    // console.log("No result")
                    return
                }

                if (result.length > 0) {
                    // calculate center point for all positions
                    var lat = 0;
                    var lng = 0;
                    result.forEach( function(fileAccess) {
                        let hash = nGeohash.decode(fileAccess.geohash);
                        lat += hash.latitude;
                        lng += hash.longitude;
                    });

                    var avgLat = lat / result.length;
                    var avgLng = lng / result.length;

                    var avgGeohash = nGeohash.encode(avgLat, avgLng, GEOHASH_PRECISION);
                    
                    // update FileAccessLocation if already present,
                    // insert new otherwise
                    m.FileAccessLocation.findOne(query, function(err, res) {
                        
                        if(!res) {
                            // no FileAccessLocation entry found 
                            
                            // note: result.length is the length of outside var result
                            var fal = new m.FileAccessLocation({
                                "file": file,
                                "geohash": avgGeohash,
                                "counter": result.length,
                                "replicated": false
                            });

                            fal.save(function(err) { 
                                if (err) {
                                    console.log("[" + getDateTime() + "] Error saving new FileAccessLocation to MongoDB:");
                                    console.log(err);
                                    return;
                                }

                                console.log("[" + getDateTime() + "] New FileAccessLocation inserted into MongoDB");
                            });

                            return;
                        }

                        // update existing matching FileAccessLocation
                        m.FileAccessLocation.update({"_id": res._id}, {$set: {"geohash": avgGeohash, "counter": result.length} }, function(err) {
                            if (err) {
                                console.log("[" + getDateTime() + "] Error updating FileAccessLocation in MongoDB:");
                                console.log(err);
                                return;
                            }

                            console.log("[" + getDateTime() + "] Updated FileAccessLocation in MongoDB");
                        });

                    });

                } else {
                    console.log("[" + getDateTime() + "] No FileAccess entries in MongoDB!")  // should never be the case
                }
            });

        });

    }

    // cron-job running regularly to identify which files to replicate
    cron.schedule('*/10 * * * * *', function() {
        // console.log("[" + getDateTime() + "] Replication cron job running!")

        fileLog(eventTypes.REPLICATION_CRONJOB, "", "", "");

        // get all FileAccessLocations that are above counter threshold and have not yet been replicated
        m.FileAccessLocation.find({counter: {$gt: REPLICATION_COUNTER_THRESHOLD}, replicated: false}, function(err, fileAccessLocations) {

            if (err) {
                console.log("[" + getDateTime() + "] Error retrieving FileAccessLocations from MongoDB:");
                console.log(err);
                return;
            }

            if (fileAccessLocations.length < 1 ) {
                return;
            }

            // else: start replicating

            // get nodes from MasterNode
            var nodesArray = [];
            request(MASTER_NODE_URL + ":" + MASTER_NODE_PORT + "/" + MASTER_API_VERSION + "/nodes", function (error, response, body) {
                if (error) {
                    console.log("[" + getDateTime() + "] Error retrieving nodes from MasterNode:");
                    console.log(error);
                    return;
                }
                nodesArray = JSON.parse(body).data.nodes;

                var counter = 0;
                var fileNodeSet = new Set();
                var fileLocationMap = {};
                
                fileAccessLocations.forEach(function(fal) {
                    // file to replicate:
                    var fileUuid = fal.file;
    
                    // spatial destination:
                    var dest = fal.geohash;
                    var latlong = nGeohash.decode(dest);
                    
                    // for current fileAccessLocation, find node that is spatially nearest to fal.latlong
                    var minDistance = Number.MAX_VALUE;
                    var targetNode = null;
                    nodesArray.forEach( function(node) {
                        var dist = distanceBetween(node.location, [latlong.latitude, latlong.longitude]);

                        if (dist < minDistance) {
                            minDistance = dist;
                            targetNode = node;
                        }
                    });

                    if (!targetNode) {
                        return;
                    }

                    // TODO: check if targetNode is this node --> if so: abort
                    
                    // reduce number of replication requests of same file to same node to 1
                    var fileNodePair = fileUuid + "###" + targetNode.url + ":" + targetNode.port; // + "###" + fal.geohash.substring(0, GEOHASH_COMPARE_PRECISION);
                    if (fileNodeSet.has(fileNodePair)) {
                        fileLocationMap[fileNodePair].push(fal.geohash.substring(0, GEOHASH_COMPARE_PRECISION));
                    } else {
                        fileNodeSet.add(fileNodePair);
                        fileLocationMap[fileNodePair] = [fal.geohash.substring(0, GEOHASH_COMPARE_PRECISION)];
                    }
                                        
                    counter++;

                    if (counter == fileAccessLocations.length) {
                        // all file-node pairs should have been checked now and duplicated removed
                        // var fileNodes = fileNodeSet.toArray();  // transform Set to Array to loop through it
                        var fileNodes = Array.from(fileNodeSet);
                        for (var i = 0; i < fileNodes.length; i++) {
                            var parts = fileNodes[i].split("###");
                            var fileUuid = parts[0];
                            var nodeUrlPort = parts[1];
                            
                            // submit geohash_prefixes to replication request
                            var geohashPrefix = fileLocationMap[ fileNodes[i] ];

                            // send file to targetNode
                            gfs.exist({filename: fileUuid}, function(error, found) {

                                if (error || !found) {
                                    console.log("[" + getDateTime() + "] File not found on this node: " + fileUuid);
                                    return;
                                }

                                m.File.findOne({uuid: fileUuid}, function(error, file){
                                    // send file to specified node
                                    if (error) {
                                        console.log("[" + getDateTime() + "] Could not find file " + fileUuid + " in MongoDB!");
                                        return;
                                    }

                                    // write file to tmp directory to create working filestreams
                                    try {
                                        var fsstreamwrite = fs.createWriteStream(tmp_replication_dir + "/" + fileUuid);
                                        var readstream = gfs.createReadStream( {filename: fileUuid} );
                                        readstream.pipe(fsstreamwrite);
                                    } catch (err) {
                                        console.log("[" + getDateTime() + "] Error directing streams for file " + fileUuid);
                                    }
                                    
                                    // readstream.on("error", function() { 
                                    //     console.log("[" + getDateTime() + "] Error on readStrema for file " + fileUuid);
                                    //     return;
                                    //  });

                                    readstream.on("close", function () {
                                        // console.log("File Read successfully from database");

                                        try {
                                            var options = {
                                                url: nodeUrlPort + "/replication/data",
                                                method: "POST",
                                                enctype: "multipart/form-data",
                                                formData: {
                                                    "filedata": fs.createReadStream(tmp_replication_dir + "/" + fileUuid),
                                                    "metadata": JSON.stringify(file),
                                                    "src_port": NODE_PORT,
                                                    "geohash_prefix": geohashPrefix 
                                                }
                                            }
                                        } catch (err) {
                                            console.log("[" + getDateTime() + "] Error creating readstream for file " + fileUuid);
                                            console.log("               --> Abort.");
                                            return;
                                        }
                                        
                                        fileLog(eventTypes.TRIGGER_REPLICATION, fileUuid, "", nodeUrlPort);

                                        request(options, function(error, response, body) {
                                            if (error) {
                                                console.log("[" + getDateTime() + "] Error replicating file:");
                                                console.log(error);
                                                return;
                                            }

                                            // delete temporary file
                                            fs.unlink(tmp_replication_dir + "/" + fileUuid, function(err){
                                                // file not present anymore. Continue...
                                            });                      

                                        });

                                    });

                                });

                            }); 
                    
                        }
                    }

                    // update FileAccessLocation to replicated = true
                    m.FileAccessLocation.updateOne({"_id": fal._id}, {$set: {"replicated": true} }, function(err){
                        if (err) {
                            console.log("[" + getDateTime() + "] Error updating FileAccessLocation after Replication:");
                            console.log(err);
                            return;
                        }
                        
                    });
             
                });

            });

        });

    });


    // cron-job sorting out replicated files that have not been accessed 
    // for more than REPLICATION_RETAINING_THRESHOLD days
    
    cron.schedule("*/15 * * * * *", function() {

        fileLog(eventTypes.RETAINING_CRONJOB, "", "", "");

        // transform retaining threshold into milliseconds, as this value is saved in the db
        var retaining_ms = REPLICATION_RETAINING_THRESHOLD * 24 * 60 * 60 * 1000;

        // ADAPTION OF TIME THRESHOLD FOR SIMULATION: 15s
        retaining_ms = 15000;

        // get all replicated files that exceed the threshold
        m.ReplicatedFile.find({"lastAccess": {$lt: Date.now() - retaining_ms}}, function(err, replications) {

            if (err) {
                console.log("[" + getDateTime() + "] Error retrieving outdated replications from DB:");
                console.log(err);
                return;
            }

            if (replications && replications.length > 0) {

                replications.forEach( function(replicatedFile) {

                    fileLog(eventTypes.DELETE_REPLICATION, replicatedFile._id, "", "");

                    // delete file from MongoDB
                    m.File.deleteMany({"uuid": replicatedFile._id}, function(err){ });

                    // delete file and thumbnail from gridFS
                    gfs.remove({"filename": replicatedFile._id}, function(err){ });
                    gfs.remove({"filename": "thumb_" + replicatedFile._id}, function(err){ });

                    // inform master node about deletion
                    var removedNodeMapping = {
                        file_id: replicatedFile._id,
                        node_id: NODE_UUID
                    }
                    var payload = {
                        url: MASTER_NODE_URL + ":" + MASTER_NODE_PORT + "/" + MASTER_API_VERSION + "/remove_node",
                        method: "DELETE",
                        json: removedNodeMapping
                    }
                    request.delete(payload, function(err, res, body){
                        if(err) {
                            console.log("["+getDateTime()+"] Cannot inform master node about file mapping!");
                            return;
                        }
            
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            console.log("["+getDateTime()+"] Updated mapping on master node");
                        }
                    });

                    // inform source node to reset FileAccessLocations
                    var resetData = {
                        file: replicatedFile._id,
                        geohash_prefix: replicatedFile.geohash_prefix
                    }
                    payload = {
                        url: "http://[" + replicatedFile.src_address + "]:" + replicatedFile.src_port + "/replication/reset",
                        method: "POST",
                        json: resetData
                    }

                    request.post(payload, function(err, res, body) {
                        if (err) {
                            console.log("["+getDateTime()+"] Cannot inform source node about FAL reset!");
                            return;
                        }
                        
                    });

                    // delete replication entry
                    replicatedFile.remove();

                });
            }

        });
        
    });

    // distance between two sets of coordinates, according to haversine formula:
    function degreesToRadians(degrees) {
        return degrees * Math.PI / 180;
    }
      
    function distanceBetween(loc1, loc2) {
        var earthRadiusKm = 6371;
        
        var lat1 = loc1[0];
        var lon1 = loc1[1];
        var lat2 = loc2[0];
        var lon2 = loc2[1];

        var dLat = degreesToRadians(lat2-lat1);
        var dLon = degreesToRadians(lon2-lon1);
        
        lat1 = degreesToRadians(lat1);
        lat2 = degreesToRadians(lat2);
        
        var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2); 
        var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
        return earthRadiusKm * c;
    }

    function updateMappingOnMasterNode(replicatedFile) {

        var nodeMapping = {
            device_id: replicatedFile.phoneID,
            file_id: replicatedFile.uuid,
            node_id: NODE_UUID
        }
        var payload = {
            url: MASTER_NODE_URL + ":" + MASTER_NODE_PORT + "/" + MASTER_API_VERSION + "/file_node_mapping",
            method: "POST",
            json: nodeMapping
        }

        request.post(payload, function(err, res, body){
            if(err) {
                console.log("["+getDateTime()+"] Cannot inform master node about file mapping!")
                return;
            }

            if (res.statusCode >= 200 && res.statusCode < 300) {
                console.log("["+getDateTime()+"] Updated mapping on master node");
            }
            
        });

    }

    function fileLog(eventType, vstoreUuid, obj_id, targetNode) {

        // "Timestamp, NODE_PORT, EventType, vStore-UUID, Obj_id, targetNode\n"
        var logContent = (new Date().toISOString()) + "," + NODE_PORT + ", " + eventType + "," + vstoreUuid + "," + obj_id + "," + targetNode + "\n";

        logStream.write(logContent);
    }

    //*************************//
    //*** REPLICATION ABOVE ***//
    //*************************//

    /**
     * Tries to parse the given json string into a ContextSchema
     */
    function parseCtxToModel(stringJsonContext)
    {
        //Fill models with data coming from context
        var context = new m.Context;

        var ctx = JSON.parse(stringJsonContext);

        if(ctx.location)
        {
            context.location = new m.Location(
            {
                "description" : ctx.location.description,
                "time" : ctx.location.time,
                "loc" : [ctx.location.lng, ctx.location.lat]
            });
        }
        if(ctx.places)
        {
            context.places = new m.Places;
            context.places.time = ctx.places.time;
            for(var key in ctx.places.places)
            {
                var p = ctx.places.places[key];
                var place = new m.Place(
                {
                    "id" : p.id,
                    "name" : p.name,
                    "loc" : new m.Location(
                    {
                        "loc" : [p.lng, p.lat]
                    }),
                    "type" : p.type,
                    "category": p.category,
                    "likelihood" : p.likelihood
                });
                context.places.places.push(place);
            }
        }
        if(ctx.place)
        {
            context.place = ctx.place;
        }
        if(ctx.activity)
        {
            context.activity = ctx.activity.activity;
        }
        if(ctx.noise)
        {
            context.noise = new m.Noise({
                "soundDb": ctx.noise.sound_db,
                "soundRms": ctx.noise.sound_rms,
                "isSilent": ctx.noise.isSilent,
                "time": ctx.noise.time,
            });
        }
        if(ctx.network)
        {
            context.network = new m.Network({
                "isWifiConnected": ctx.network.isWifiConnected,
                "wifiSsid": ctx.network.wifiSsid,
                "mobileNetworkType": ctx.network.mobileNetworkType,
            });
        }
        if(ctx.weekday)
        {
            context.weekday = ctx.weekday;
        }
        if(ctx.timestamp) {
            var date = new Date(ctx.timestamp);
            context.time = date;
            context.hours = date.getHours();
            context.minutes = date.getMinutes();
        }

        return context;
    }
};
