# StorageNode for the vStore Framework

A sample implementation of a storage node for the *vStore* framework. Please refer to the framework's [main repository](https://github.com/Telecooperation/vstore-framework) and [Wiki](https://github.com/Telecooperation/vstore-framework/wiki) for further documentation. 

## Installation on a Raspberry Pi

### Install node.js:

- ``curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -``
- ``sudo apt-get install -y nodejs`` to install node.js 8.x and npm

(For older Raspberry Pi (e.g. <= Pi2A): https://raspberrypi.stackexchange.com/questions/48303/install-nodejs-for-all-raspberry-pi)

### Install MongoDB
- ``sudo apt-get install mongodb-server``
- MongoDB Config file: ``sudo nano /etc/mongodb.conf``
- ``sudo service mongodb start``

### Setup StorageNode server

Install tools needed for thumbnail creation:
- ``sudo apt-get install libav-tools graphicsmagick``

Clone the repository:
- ``git clone https://github.com/Telecooperation/vstore-node``
- ``cd vstore-node``

After cloning the repository, run ``npm install`` in the directory to install necessary modules.

Configure the port number and storage node type in the file ``app.js``.

To run the node.js app in foreground:
- ``node app.js``

To run node.js in the background:
- Install screen: ``sudo apt-get install screen``
- ``Start new screen: screen -dmS <screen name>``
- ``screen -S <screen name> -X stuff 'node app.js\n'`` (yes, stuff and \n are necessary!)

Attach to screen:
- ``screen -r <screen name>``

Kill screen:
- ``screen -S <screen name> -X quit``

## Using the admin interface to MongoDB backend
To access the MongoDB database more easily and graphically, we use [mongo-express](https://github.com/mongo-express/mongo-express). You can easily add, remove and view file information.

This can be called by visiting `http://<address_of_node>:<port>/admin`. The login credentials can be configured in the file [mongo_express_config.js](https://github.com/Telecooperation/vstore-node/blob/master/mongo_express_config.js) by changing the variables `mongoexpressUser` and `mongoexpressPass`.
