module.exports = function(mongoose) {
    var Schema = mongoose.Schema;
    
    //Define all the mongoose models for context types
    var LocationSchema = Schema(
    {
        description: String,
        time: Number,
        loc: {
            type: [Number], // [<longitude>, <latitude>]
            index: '2dsphere' // create the 2d geospatial index
        }
    });

    var PlaceSchema = Schema(
    {
        id: String,
        name: String,
        loc: LocationSchema,
        type: Number,
        category: String,
        likelihood: Number
    });
    
    var PlacesSchema = Schema(
    {
        places: [PlaceSchema],
        time: Number
    });
    
    var NoiseSchema = Schema(
    {
        soundDb: Number,
        soundRms: Number,
        isSilent: Boolean,
        time: Number
    });
    
    var NetworkSchema = Schema(
    {
        isWifiConnected: Boolean,
        wifiSsid: String,
        mobileNetworkType: String
    });

    var ContextSchema = Schema(
    {
        location: LocationSchema,
        places: PlacesSchema,
        place: String,
        activity: Number,
        noise: NoiseSchema,
        network: NetworkSchema,
        weekday: Number,
        time: Date,
        hours: Number,
        minutes: Number
    });
    
    var FileSchema = Schema(
    {
        uuid: String,
        md5: String,
        descriptiveName: String,
        mimetype: String,
        extension: String,
        filesize: Number,
        creationTimestamp: Number,
        context: ContextSchema,
        isPrivate: Boolean,
        phoneID: String
    });

    var FileAccessSchema = Schema(
    {
        uuid: String,
        file: String,
        geohash: String,
        timeOfWeek: String,
        totalMinutes: Number,
        deviceId: String
    });

    var FileAccessLocationSchema = Schema(
    {
        geohash: String,
        counter: Number,
        file: String,
        replicated: Boolean
    });

    var models = {
        Location : mongoose.model('Location', LocationSchema),
        Place : mongoose.model('Place', PlaceSchema),
        Places : mongoose.model('Places', PlacesSchema),
        Noise : mongoose.model('Noise', NoiseSchema),
        Network : mongoose.model('Network', NetworkSchema),
        Context : mongoose.model('Context', ContextSchema),
        File : mongoose.model('File', FileSchema),
        FileAccess: mongoose.model('FileAccess', FileAccessSchema),
        FileAccessLocation: mongoose.model('FileAccessLocation', FileAccessLocationSchema)
    };
    return models;
}
