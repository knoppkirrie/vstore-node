var NODE_PORT = 50001;
var NODE_TYPE = "CLOUDLET";

const uuidv4 = require('uuid/v4');
var express = require('express');
var multer  = require('multer')
var upload = multer({ dest: './tmp_upload/' })

var mongoose = require('mongoose');
mongoose.Promise = global.Promise;
mongoose.connect('mongodb://127.0.0.1/vstore_node');
var dbConn = mongoose.connection;
var Grid = require('gridfs-stream');
Grid.mongo = mongoose.mongo;

var app = express();
var port = process.env.PORT || NODE_PORT;

// Middleware for MongoDB admin interface
var mongo_express = require('mongo-express/lib/middleware')
var mongo_express_config = require('./mongo_express_config.js')
app.use('/admin', mongo_express(mongo_express_config))

dbConn.once('open', function() {
    console.log("Mongoose DB connection to mongoDB now open!");
});

var ffmpeg = require('fluent-ffmpeg');
ffmpeg.setFfmpegPath('/usr/bin/avconv');

var NODE_UUID;

//Check if we have generated a UUID for this node.
var uuidSchema = new mongoose.Schema({configured: Boolean, uuid: String});
var UUID = mongoose.model('UUID', uuidSchema);
UUID.findOne({configured: true}, function(err, obj) {
    if(!obj) {
        //No UUID in database, so we generate one and save it.
        NODE_UUID = uuidv4();
        var new_uuid = new UUID({ configured: true, uuid: NODE_UUID});
        new_uuid.save(function (err) {
            if(err) {
                console.log(err);
                process.exit(1);
            } else {
                console.log("New UUID generated for this storage node: " + NODE_UUID);
            }  
        });
    } else {
        NODE_UUID = obj.uuid;
    }

    console.log("Server running with UUID: " + NODE_UUID);

    // Require the API routes
    var routes = require('./routes.js')(app, upload, mongoose, dbConn,
    NODE_UUID, NODE_TYPE, ffmpeg);

});


// Start server
app.listen(port);
console.log('Listening on port ' + port + '...');
