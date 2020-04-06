/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2020 Kuzzle
 * mailto: support AT kuzzle.io
 * website: http://kuzzle.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const Bluebird = require('bluebird');
const debug = require('debug')('kuzzle:cluster');
const IORedis = require('ioredis');
const { v4: uuidv4 } = require('uuid');
const Node = require('./node');
const ClusterRealtime = require('./realtime');

IORedis.Promise = Bluebird;

class KuzzleCluster {
  constructor (kuzzle) {
    this.kuzzle = kuzzle;
    this.config = kuzzle.config;

    this.uuid = uuidv4();
    this.node = new Node(this);
    this.realtime = new ClusterRealtime(kuzzle);

    this.redis = Array.isArray(this.config.redis)
      ? new IORedis.Cluster(this.config.redis)
      : new IORedis(this.config.redis);

    this.redis.defineCommand('clusterCleanNode', {
      lua: fs.readFileSync(path.resolve(__dirname, 'redis/cleanNode.lua')),
      numberOfKeys: 1,
    });
    this.redis.defineCommand('clusterState', {
      lua: fs.readFileSync(path.resolve(__dirname, 'redis/getState.lua')),
      numberOfKeys: 1,
    });
    this.redis.defineCommand('clusterSubOn', {
      lua: fs.readFileSync(path.resolve(__dirname, 'redis/subon.lua')),
      numberOfKeys: 1,
    });
    this.redis.defineCommand('clusterSubOff', {
      lua: fs.readFileSync(path.resolve(__dirname, 'redis/suboff.lua')),
      numberOfKeys: 1,
    });


    this.hooks = {
      'core:kuzzleStart': 'kuzzleStarted',
      'core:notify:document': 'notifyDocument',
      'core:notify:user': 'notifyUser',
      'core:profileRepository:delete': 'profileUpdated',
      'core:profileRepository:save': 'profileUpdated',
      'core:roleRepository:delete': 'roleUpdated',
      'core:roleRepository:save': 'roleUpdated',
      'realtime:errorSubscribe': 'unlockCreateRoom',
      'realtime:errorUnsubscribe': 'unlockDeleteRoom',
      'room:new': 'roomCreated',
      'room:remove': 'roomDeleted',
    };

    this.pipes = {
      'core:auth:strategyAdded': 'strategyAdded',
      'core:auth:strategyRemoved': 'strategyRemoved',
      'core:hotelClerk:addSubscription': 'subscriptionAdded',
      'core:hotelClerk:join': 'subscriptionJoined',
      'core:hotelClerk:removeRoomForCustomer': 'subscriptionOff',
      'realtime:beforeJoin': 'beforeJoin'
    };

    this._shutdown = false;
  }

  get ready () {
    return this.node.ready;
  }

  get broadcast () {
    return this.node.broadcast;
  }

  // --------------------------------------------------------------------------
  // hooks
  // --------------------------------------------------------------------------

  /**
   * @param {Request} request
   * @param {function} cb callback
   * @param {integer} attempts
   */
  beforeJoin (request, cb, attempts = 0) {
    if (!request.input.body || !request.input.body.roomId) {
      return cb(null, request);
    }

    const roomId = request.input.body.roomId;

    if (this.kuzzle.hotelClerk.rooms.has(roomId)) {
      return cb(null, request);
    }

    const room = this._rooms.flat.get(roomId);

    if (room) {
      this.kuzzle.hotelClerk.rooms.set(roomId, {
        channels: {},
        collection: room.collection,
        customers: new Set(),
        id: roomId,
        index: room.index,
      });

      return cb(null, request);
    }

    // room not found. May be normal but can also be due to cluster state
    // propagation delay
    if (attempts > 0) {
      return cb(null, request);
    }

    setTimeout(
      () => this.beforeJoin(request, cb, attempts + 1),
      this.config.timers.joinAttemptInterval);
  }

  kuzzleStarted () {
    // register existing strategies
    const promises = [];
    for (const name of this.kuzzle.pluginsManager.listStrategies()) {
      const strategy = this.kuzzle.pluginsManager.strategies[name];

      promises.push(
        this.redis.hset(
          'cluster:strategies',
          name,
          JSON.stringify({
            plugin: strategy.owner,
            strategy: strategy.strategy
          })));
    }

    return Bluebird.all(promises)
      .then(() => {
        this._isKuzzleStarted = true;
        return this.node.init();
      });
  }

  notifyDocument (data) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast document notification: node not connected to cluster', this.uuid);
      return;
    }

    this.node.broadcast('cluster:notify:document', data);
  }

  notifyUser (data) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast user notification: node not connected to cluster', this.uuid);
      return;
    }

    this.node.broadcast('cluster:notify:user', data);
  }

  /**
   * @param {object} diff
   */
  profileUpdated (diff) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "profile update" action: node not connected to cluster', this.uuid);
      return;
    }

    this.node.broadcast('cluster:sync', {
      event: 'profile',
      id: diff._id
    });
  }

  /**
   * @param {object} diff
   */
  roleUpdated (diff) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "role update" action: node not connected to cluster', this.uuid);
      return;
    }

    this.node.broadcast('cluster:sync', {
      event: 'role',
      id: diff._id
    });
  }

  roomCreated (payload) {
    this.node.state.locks.create.add(payload.roomId);
  }

  roomDeleted (roomId) {
    this.node.state.locks.delete.add(roomId);
  }

  strategyAdded (payload) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "strategy added" action: node not connected to cluster', this.uuid);
      return Bluebird.resolve(payload);
    }

    return this.redis
      .hset('cluster:strategies', payload.name, JSON.stringify({
        plugin: payload.pluginName,
        strategy: payload.strategy
      }))
      .then(() => this.node.broadcast('cluster:sync', {event: 'strategies'}))
      .then(() => payload);
  }

  strategyRemoved (payload) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "strategy added" action: node not connected to cluster', this.uuid);
      return Bluebird.resolve(payload);
    }

    return this.redis.hdel('cluster:strategies', payload.name)
      .then(() => this.node.broadcast('cluster:sync', {event: 'strategies'}))
      .then(() => payload);
  }

  subscriptionAdded (diff) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "subscription added" action: node not connected to cluster', this.uuid);
      return Bluebird.resolve(diff);
    }

    const
      {
        index,
        collection,
        filters,
        roomId,
        connectionId
      } = diff,
      filter = {collection, filters, index},
      serializedFilter = filters && JSON.stringify(filter) || 'none';

    debug('[hook] sub add %s/%s', roomId, connectionId);

    let result;
    return this.redis
      .clusterSubOn(
        `{${index}/${collection}}`,
        this.uuid,
        roomId,
        connectionId,
        serializedFilter)
      .then(r => {
        result = r;
        return this.redis.sadd('cluster:collections', `${index}/${collection}`);
      })
      .then(() => this._onSubOn('add', index, collection, roomId, result))
      .then(() => diff)
      .finally(() => this.node.state.locks.create.delete(roomId));
  }

  subscriptionJoined (diff) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "subscription joined" action: node not connected to cluster', this.uuid);
      return Bluebird.resolve(diff);
    }

    const
      {
        index,
        collection,
        roomId,
        connectionId
      } = diff;

    if (diff.changed === false) {
      debug('[hook][sub joined] no change');
      return Bluebird.resolve(diff);
    }

    return this.redis
      .clusterSubOn(
        `{${index}/${collection}}`,
        this.uuid,
        roomId,
        connectionId,
        'none')
      .then(result => this._onSubOn('join', index, collection, roomId, result))
      .then(() => diff);
  }

  subscriptionOff (object) {
    if (!this.node.ready) {
      debug('[%s][warning] could not broadcast "subscription off" action: node not connected to cluster', this.uuid);
      return Bluebird.resolve(object);
    }

    const
      room = object.room,
      {index, collection} = room,
      connectionId = object.requestContext.connectionId;

    debug('[hook] sub off %s/%s', room.id, connectionId);

    return this.redis
      .clusterSubOff(
        `{${room.index}/${room.collection}}`,
        this.uuid,
        room.id,
        connectionId)
      .then(result => {
        const [version, count] = result;

        if (this.node.state.getVersion(index, collection) < version) {
          this.setRoomCount(index, collection, room.id, count);
        }

        debug(
          '[hook][sub off] v%d %s/%s/%s -%s = %d',
          version,
          index,
          collection,
          room.id,
          connectionId,
          count);

        return this.node.broadcast('cluster:sync', {
          collection,
          event: 'state',
          index,
          post: 'off',
          roomId: room.id,
        });
      })
      .then(() => object)
      .finally(() => this.node.state.locks.delete.delete(room.id));
  }

  /**
   * @param {Request} request
   */
  unlockCreateRoom (request) {
    // incoming request can be invalid. We need to check for its params
    if (!request.input.body || !request.input.body.roomId) {
      return;
    }

    this.node.state.locks.create.delete(request.input.body.roomId);
  }

  /**
   * @param {Request} request
   */
  unlockDeleteRoom (request) {
    // incoming request can be invalid. We need to check for its params
    if (!request.input.body || !request.input.body.roomId) {
      return;
    }

    this.node.state.locks.delete.delete(request.input.body.roomId);
  }


  // --------------------------------------------------------------------------
  // business
  // --------------------------------------------------------------------------
  /**
   * Removes cluster related data inserted in redis from nodeId
   *
   * @param {string} nodeId
   */
  async cleanNode (node) {
    const promises = [];

    await this.redis.srem('cluster:discovery', JSON.stringify({
      pub: node.pub,
      router: node.router
    }));

    if (node === this.node && this.node.pool.size === 0) {
      debug('last node to quit.. cleaning up');
      await this.node.state.reset();
    }
    else {
      for (const [index, collections] of this._rooms.tree.entries()) {
        for (const collection of collections.keys()) {
          promises.push(
            this.redis.clusterCleanNode(
              `{${index}/${collection}}`,
              node.uuid));
        }
      }

      await Bluebird.all(promises);
    }

    return this.node.broadcast('cluster:sync', {event: 'state:all'});
  }

  deleteRoomCount (roomId) {
    const room = this._rooms.flat.get(roomId);
    if (!room) {
      return;
    }

    const { index, collection } = room;

    this._rooms.flat.delete(roomId);

    const
      collections = this._rooms.tree.get(index),
      rooms = collections.get(collection);

    rooms.delete(roomId);

    if (rooms.size === 0) {
      collections.delete(collection);

      if (collections.size === 0) {
        this._rooms.tree.delete(index);
      }
    }
  }

  reset () {
    return this.node.state.reset()
      .then(() => this.node.state.syncAll({post: 'reset'}))
      .then(() => {
        this._rooms.flat.clear();
        this._rooms.tree.clear();
      });
  }

  setRoomCount (index, collection, roomId, _count) {
    const count = parseInt(_count, 10);

    if (count === 0) {
      return this.deleteRoomCount(roomId);
    }

    const val = {
      collection,
      count,
      index,
    };

    this._rooms.flat.set(roomId, val);

    let collections = this._rooms.tree.get(index);

    if (!collections) {
      collections = new Map();
      this._rooms.tree.set(index, collections);
    }

    if (!collections.has(collection)) {
      collections.set(collection, new Set());
    }

    collections.get(collection).add(roomId);
  }

  _onSubOn (type, index, collection, roomId, result) {
    const [version, count] = result;

    if (this.node.state.getVersion(index, collection) < version) {
      this.setRoomCount(index, collection, roomId, count);
    }

    debug('[hook][sub %s] v%d %s/%s/%s = %d',
      type,
      version,
      index,
      collection,
      roomId,
      count);

    return this.node.broadcast('cluster:sync', {
      collection,
      event: 'state',
      index,
      post: type,
      roomId,
    });
  }

  async shutdown () {
    if (this._shutdown) {
      return;
    }

    this._shutdown = true;

    await this.cleanNode(this.node);
  }
}

module.exports = KuzzleCluster;
