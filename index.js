/**
 * Module dependencies.
 */

const uid2 = require('uid2');
const redis = require('redis').createClient;
const msgpack = require('notepack.io');
const Adapter = require('socket.io-adapter');
const debug = require('debug')('socket.io-mqtt');

/**
 * Request types, for messages between nodes
 */

const requestTypes = {
  clients: 0,
  clientRooms: 1,
  allRooms: 2,
  remoteJoin: 3,
  remoteLeave: 4,
  customRequest: 5,
  remoteDisconnect: 6
};

/**
 * Returns a redis Adapter class.
 *
 * @param {String} optional, redis uri
 * @return {RedisAdapter} adapter
 * @api public
 */

module.exports = function adapter(uri, opts) {
  opts = opts || {};

  // handle options only
  if (typeof uri === 'object') {
    opts = uri;
    uri = null;
  }

  // opts
  let pub = opts.pubClient;
  let sub = opts.subClient;
  const prefix = opts.key || 'socket.io';
  const requestsTimeout = opts.requestsTimeout || 5000;

  // init clients if needed
  function createClient() {
    if (uri) {
      // handle uri string
      return redis(uri, opts);
    } else {
      return redis(opts);
    }
  }

  if (!pub) pub = createClient();
  if (!sub) sub = createClient();

  // this server's key
  const uid = uid2(6);

  /**
   * Adapter constructor.
   *
   * @param {String} namespace name
   * @api public
   */

  function MQTT(nsp) {
    Adapter.call(this, nsp);

    this.uid = uid;
    this.prefix = prefix;
    this.requestsTimeout = requestsTimeout;

    this.channel = `${prefix}#${nsp.name}#`;
    this.requestChannel = `${prefix}-request#${this.nsp.name}#`;
    this.responseChannel = `${prefix}-response#${this.nsp.name}#`;
    this.requests = {};
    this.customHook = function(data, cb) {
      cb(null);
    };

    this.channelMatches = function(messageChannel, subscribedChannel) {
      return messageChannel.startsWith(subscribedChannel);
    };

    this.pubClient = pub;
    this.subClient = sub;

    const self = this;

    sub.psubscribe(`${this.channel}*`, function(err) {
      if (err) self.emit('error', err);
    });

    sub.on('pmessageBuffer', this.onmessage.bind(this));

    sub.subscribe([this.requestChannel, this.responseChannel], function(err) {
      if (err) self.emit('error', err);
    });

    sub.on('messageBuffer', this.onrequest.bind(this));

    function onError(err) {
      self.emit('error', err);
    }
    pub.on('error', onError);
    sub.on('error', onError);
  }

  /**
   * Inherits from `Adapter`.
   */

  MQTT.prototype.__proto__ = Adapter.prototype;

  /**
   * Called with a subscription message
   *
   * @api private
   */

  MQTT.prototype.onmessage = function(pattern, channel, msg) {
    channel = channel.toString();

    if (!this.channelMatches(channel, this.channel)) {
      return debug('ignore different channel');
    }

    const room = channel.slice(this.channel.length, -1);
    if (room !== '' && !this.rooms.hasOwnProperty(room)) {
      return debug('ignore unknown room %s', room);
    }

    const args = msgpack.decode(msg);
    let packet;

    if (uid === args.shift()) return debug('ignore same uid');

    packet = args[0];

    if (packet && packet.nsp === undefined) {
      packet.nsp = '/';
    }

    if (!packet || packet.nsp != this.nsp.name) {
      return debug('ignore different namespace');
    }

    args.push(true);

    this.broadcast.apply(this, args);
  };

  /**
   * Called on request from another node
   *
   * @api private
   */

  MQTT.prototype.onrequest = function(channel, msg) {
    channel = channel.toString();

    if (this.channelMatches(channel, this.responseChannel)) {
      return this.onresponse(channel, msg);
    } else if (!this.channelMatches(channel, this.requestChannel)) {
      return debug('ignore different channel');
    }

    const self = this;
    let request;

    try {
      request = JSON.parse(msg);
    } catch (err) {
      self.emit('error', err);
      return;
    }

    debug('received request %j', request);

    switch (request.type) {
      case requestTypes.clients:
        Adapter.prototype.clients.call(self, request.rooms, function(err, clients) {
          if (err) {
            self.emit('error', err);
            return;
          }

          const response = JSON.stringify({
            requestid: request.requestid,
            clients
          });

          pub.publish(self.responseChannel, response);
        });
        break;

      case requestTypes.clientRooms:
        Adapter.prototype.clientRooms.call(self, request.sid, function(err, rooms) {
          if (err) {
            self.emit('error', err);
            return;
          }

          if (!rooms) {
            return;
          }

          const response = JSON.stringify({
            requestid: request.requestid,
            rooms
          });

          pub.publish(self.responseChannel, response);
        });
        break;

      case requestTypes.allRooms:
        var response = JSON.stringify({
          requestid: request.requestid,
          rooms: Object.keys(this.rooms)
        });

        pub.publish(self.responseChannel, response);
        break;

      case requestTypes.remoteJoin:
        var socket = this.nsp.connected[request.sid];
        if (!socket) {
          return;
        }

        socket.join(request.room, function() {
          const response = JSON.stringify({
            requestid: request.requestid
          });

          pub.publish(self.responseChannel, response);
        });
        break;

      case requestTypes.remoteLeave:
        var socket = this.nsp.connected[request.sid];
        if (!socket) {
          return;
        }

        socket.leave(request.room, function() {
          const response = JSON.stringify({
            requestid: request.requestid
          });

          pub.publish(self.responseChannel, response);
        });
        break;

      case requestTypes.remoteDisconnect:
        var socket = this.nsp.connected[request.sid];
        if (!socket) {
          return;
        }

        socket.disconnect(request.close);

        var response = JSON.stringify({
          requestid: request.requestid
        });

        pub.publish(self.responseChannel, response);
        break;

      case requestTypes.customRequest:
        this.customHook(request.data, function(data) {
          const response = JSON.stringify({
            requestid: request.requestid,
            data
          });

          pub.publish(self.responseChannel, response);
        });

        break;

      default:
        debug('ignoring unknown request type: %s', request.type);
    }
  };

  /**
   * Called on response from another node
   *
   * @api private
   */

  MQTT.prototype.onresponse = function(channel, msg) {
    const self = this;
    let response;

    try {
      response = JSON.parse(msg);
    } catch (err) {
      self.emit('error', err);
      return;
    }

    const requestid = response.requestid;

    if (!requestid || !self.requests[requestid]) {
      debug('ignoring unknown request');
      return;
    }

    debug('received response %j', response);

    const request = self.requests[requestid];

    switch (request.type) {
      case requestTypes.clients:
        request.msgCount++;

        // ignore if response does not contain 'clients' key
        if (!response.clients || !Array.isArray(response.clients)) return;

        for (var i = 0; i < response.clients.length; i++) {
          request.clients[response.clients[i]] = true;
        }

        if (request.msgCount === request.numsub) {
          clearTimeout(request.timeout);
          if (request.callback)
            process.nextTick(request.callback.bind(null, null, Object.keys(request.clients)));
          delete self.requests[requestid];
        }
        break;

      case requestTypes.clientRooms:
        clearTimeout(request.timeout);
        if (request.callback) process.nextTick(request.callback.bind(null, null, response.rooms));
        delete self.requests[requestid];
        break;

      case requestTypes.allRooms:
        request.msgCount++;

        // ignore if response does not contain 'rooms' key
        if (!response.rooms || !Array.isArray(response.rooms)) return;

        for (var i = 0; i < response.rooms.length; i++) {
          request.rooms[response.rooms[i]] = true;
        }

        if (request.msgCount === request.numsub) {
          clearTimeout(request.timeout);
          if (request.callback)
            process.nextTick(request.callback.bind(null, null, Object.keys(request.rooms)));
          delete self.requests[requestid];
        }
        break;

      case requestTypes.remoteJoin:
      case requestTypes.remoteLeave:
      case requestTypes.remoteDisconnect:
        clearTimeout(request.timeout);
        if (request.callback) process.nextTick(request.callback.bind(null, null));
        delete self.requests[requestid];
        break;

      case requestTypes.customRequest:
        request.msgCount++;

        request.replies.push(response.data);

        if (request.msgCount === request.numsub) {
          clearTimeout(request.timeout);
          if (request.callback)
            process.nextTick(request.callback.bind(null, null, request.replies));
          delete self.requests[requestid];
        }
        break;

      default:
        debug('ignoring unknown request type: %s', request.type);
    }
  };

  /**
   * Broadcasts a packet.
   *
   * @param {Object} packet to emit
   * @param {Object} options
   * @param {Boolean} whether the packet came from another node
   * @api public
   */

  MQTT.prototype.broadcast = function(packet, opts, remote) {
    packet.nsp = this.nsp.name;
    if (!(remote || (opts && opts.flags && opts.flags.local))) {
      const msg = msgpack.encode([uid, packet, opts]);
      let channel = this.channel;
      if (opts.rooms && opts.rooms.length === 1) {
        channel += `${opts.rooms[0]}#`;
      }
      debug('publishing message to channel %s', channel);
      pub.publish(channel, msg);
    }
    Adapter.prototype.broadcast.call(this, packet, opts);
  };

  /**
   * Gets a list of clients by sid.
   *
   * @param {Array} explicit set of rooms to check.
   * @param {Function} callback
   * @api public
   */

  MQTT.prototype.clients = function(rooms, fn) {
    if (typeof rooms === 'function') {
      fn = rooms;
      rooms = null;
    }

    rooms = rooms || [];

    const self = this;
    const requestid = uid2(6);

    pub.send_command('pubsub', ['numsub', self.requestChannel], function(err, numsub) {
      if (err) {
        self.emit('error', err);
        if (fn) fn(err);
        return;
      }

      numsub = parseInt(numsub[1], 10);
      debug('waiting for %d responses to "clients" request', numsub);

      const request = JSON.stringify({
        requestid,
        type: requestTypes.clients,
        rooms
      });

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
        const request = self.requests[requestid];
        if (fn)
          process.nextTick(
            fn.bind(
              null,
              new Error('timeout reached while waiting for clients response'),
              Object.keys(request.clients)
            )
          );
        delete self.requests[requestid];
      }, self.requestsTimeout);

      self.requests[requestid] = {
        type: requestTypes.clients,
        numsub,
        msgCount: 0,
        clients: {},
        callback: fn,
        timeout
      };

      pub.publish(self.requestChannel, request);
    });
  };

  /**
   * Gets the list of rooms a given client has joined.
   *
   * @param {String} client id
   * @param {Function} callback
   * @api public
   */

  MQTT.prototype.clientRooms = function(id, fn) {
    const self = this;
    const requestid = uid2(6);

    const rooms = this.sids[id];

    if (rooms) {
      if (fn) process.nextTick(fn.bind(null, null, Object.keys(rooms)));
      return;
    }

    const request = JSON.stringify({
      requestid,
      type: requestTypes.clientRooms,
      sid: id
    });

    // if there is no response for x second, return result
    const timeout = setTimeout(function() {
      if (fn)
        process.nextTick(
          fn.bind(null, new Error('timeout reached while waiting for rooms response'))
        );
      delete self.requests[requestid];
    }, self.requestsTimeout);

    self.requests[requestid] = {
      type: requestTypes.clientRooms,
      callback: fn,
      timeout
    };

    pub.publish(self.requestChannel, request);
  };

  /**
   * Gets the list of all rooms (accross every node)
   *
   * @param {Function} callback
   * @api public
   */

  MQTT.prototype.allRooms = function(fn) {
    const self = this;
    const requestid = uid2(6);

    pub.send_command('pubsub', ['numsub', self.requestChannel], function(err, numsub) {
      if (err) {
        self.emit('error', err);
        if (fn) fn(err);
        return;
      }

      numsub = parseInt(numsub[1], 10);
      debug('waiting for %d responses to "allRooms" request', numsub);

      const request = JSON.stringify({
        requestid,
        type: requestTypes.allRooms
      });

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
        const request = self.requests[requestid];
        if (fn)
          process.nextTick(
            fn.bind(
              null,
              new Error('timeout reached while waiting for allRooms response'),
              Object.keys(request.rooms)
            )
          );
        delete self.requests[requestid];
      }, self.requestsTimeout);

      self.requests[requestid] = {
        type: requestTypes.allRooms,
        numsub,
        msgCount: 0,
        rooms: {},
        callback: fn,
        timeout
      };

      pub.publish(self.requestChannel, request);
    });
  };

  /**
   * Makes the socket with the given id join the room
   *
   * @param {String} socket id
   * @param {String} room name
   * @param {Function} callback
   * @api public
   */

  MQTT.prototype.remoteJoin = function(id, room, fn) {
    const self = this;
    const requestid = uid2(6);

    const socket = this.nsp.connected[id];
    if (socket) {
      socket.join(room, fn);
      return;
    }

    const request = JSON.stringify({
      requestid,
      type: requestTypes.remoteJoin,
      sid: id,
      room
    });

    // if there is no response for x second, return result
    const timeout = setTimeout(function() {
      if (fn)
        process.nextTick(
          fn.bind(null, new Error('timeout reached while waiting for remoteJoin response'))
        );
      delete self.requests[requestid];
    }, self.requestsTimeout);

    self.requests[requestid] = {
      type: requestTypes.remoteJoin,
      callback: fn,
      timeout
    };

    pub.publish(self.requestChannel, request);
  };

  /**
   * Makes the socket with the given id leave the room
   *
   * @param {String} socket id
   * @param {String} room name
   * @param {Function} callback
   * @api public
   */

  MQTT.prototype.remoteLeave = function(id, room, fn) {
    const self = this;
    const requestid = uid2(6);

    const socket = this.nsp.connected[id];
    if (socket) {
      socket.leave(room, fn);
      return;
    }

    const request = JSON.stringify({
      requestid,
      type: requestTypes.remoteLeave,
      sid: id,
      room
    });

    // if there is no response for x second, return result
    const timeout = setTimeout(function() {
      if (fn)
        process.nextTick(
          fn.bind(null, new Error('timeout reached while waiting for remoteLeave response'))
        );
      delete self.requests[requestid];
    }, self.requestsTimeout);

    self.requests[requestid] = {
      type: requestTypes.remoteLeave,
      callback: fn,
      timeout
    };

    pub.publish(self.requestChannel, request);
  };

  /**
   * Makes the socket with the given id to be disconnected forcefully
   * @param {String} socket id
   * @param {Boolean} close if `true`, closes the underlying connection
   * @param {Function} callback
   */

  MQTT.prototype.remoteDisconnect = function(id, close, fn) {
    const self = this;
    const requestid = uid2(6);

    const socket = this.nsp.connected[id];
    if (socket) {
      socket.disconnect(close);
      if (fn) process.nextTick(fn.bind(null, null));
      return;
    }

    const request = JSON.stringify({
      requestid,
      type: requestTypes.remoteDisconnect,
      sid: id,
      close
    });

    // if there is no response for x second, return result
    const timeout = setTimeout(function() {
      if (fn)
        process.nextTick(
          fn.bind(null, new Error('timeout reached while waiting for remoteDisconnect response'))
        );
      delete self.requests[requestid];
    }, self.requestsTimeout);

    self.requests[requestid] = {
      type: requestTypes.remoteDisconnect,
      callback: fn,
      timeout
    };

    pub.publish(self.requestChannel, request);
  };

  /**
   * Sends a new custom request to other nodes
   *
   * @param {Object} data (no binary)
   * @param {Function} callback
   * @api public
   */

  MQTT.prototype.customRequest = function(data, fn) {
    if (typeof data === 'function') {
      fn = data;
      data = null;
    }

    const self = this;
    const requestid = uid2(6);

    pub.send_command('pubsub', ['numsub', self.requestChannel], function(err, numsub) {
      if (err) {
        self.emit('error', err);
        if (fn) fn(err);
        return;
      }

      numsub = parseInt(numsub[1], 10);
      debug('waiting for %d responses to "customRequest" request', numsub);

      const request = JSON.stringify({
        requestid,
        type: requestTypes.customRequest,
        data
      });

      // if there is no response for x second, return result
      const timeout = setTimeout(function() {
        const request = self.requests[requestid];
        if (fn)
          process.nextTick(
            fn.bind(
              null,
              new Error('timeout reached while waiting for customRequest response'),
              request.replies
            )
          );
        delete self.requests[requestid];
      }, self.requestsTimeout);

      self.requests[requestid] = {
        type: requestTypes.customRequest,
        numsub,
        msgCount: 0,
        replies: [],
        callback: fn,
        timeout
      };

      pub.publish(self.requestChannel, request);
    });
  };

  MQTT.uid = uid;
  MQTT.pubClient = pub;
  MQTT.subClient = sub;
  MQTT.prefix = prefix;
  MQTT.requestsTimeout = requestsTimeout;

  return MQTT;
};
