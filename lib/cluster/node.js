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

const Bluebird = require('bluebird');
const debug = require('../util/debug');
const debugCluster = debug('kuzzle:cluster');
const debugNotify = debug('kuzzle:cluster:notify');
const debugSync = debug('kuzzle:cluster:sync');
const {
  Request,
  errors: {InternalError: KuzzleInternalError},
} = require('kuzzle-common-objects');
const RedisStateManager = require('./redis/manager');
const zeromq = require('zeromq/v5-compat');
const ZSocket = require('./zsocket');
const Deferred = require('../util/deferred');
const NodeSync = require('./nodeSync');

class Node {
  constructor (kuzzle) {
    this.kuzzle = kuzzle;
    this.config = kuzzle.config.cluster;
    this.sync = new NodeSync(kuzzle);

    this.ready = false;

    this.sockets = {
      pub: new ZSocket('pub', this.config.bindings.pub, 7511),
      router: new ZSocket('router', this.config.bindings.router, 7510),
      sub: new ZSocket('sub'),
    };

    this.sockets.router.on('message', (envelope, binary) => {
      this._onRouterMessage(envelope, binary)
        .catch(e => {
          this.kuzzle.log.error(`Error occured when processing the envelope: ${envelope}\n${e.stack}`);
        });
    });

    this.sockets.sub.on('message', binary => this.sync.dispatch(binary));
    this.sockets.sub.subscribe('');

    // active node pool
    this.pool = new Map();

    this.state = new RedisStateManager(this);

    this.heartbeatTimer = null;
  }

  get cacheEngine () {
    return this.kuzzle.cacheEngine.internal;
  }

  /**
   * Brodcasts data to all other nodes in the cluster
   *
   * @param {string} event - the cluster event to dispatch
   * @param {object} data
   */
  async broadcast (event, data) {
    if (!this.ready) {
      debug(
        '[%s][warning] could not broadcast "%s" action: node not connected to cluster',
        event,
        this.uuid);

      return;
    }

    return sendPromise(this.sockets.pub, JSON.stringify([event, data]));
  }

  /**
   * Updates and retrieves the cluster topology
   */
  async discover () {
    debugCluster('discover %s', this.sockets.pub.href);

    await this.cacheEngine.sadd('cluster:discovery', JSON.stringify({
      pub: this.sockets.pub.href,
      router: this.sockets.router.href
    }));

    const members = await this.redis.smembers('cluster:discovery');

    for (const serialized of members) {
      this._addNode(JSON.parse(serialized));
    }

    // we are the first node to join. Clean up the db
    if (this.pool.size === 0) {
      debugCluster('%s first node in the cluster - reset', this.sockets.pub.href);
      await this.state.reset();
      return this.broadcast('cluster:sync', {event: 'state:all'});
    }
  }

  async init () {
    await Bluebird.all(Object.values(this.sockets).map(s => s.bind()));

    // We need some way to clear the timer, if needs be.
    // Currently only used by tests for clean exits
    this.heartbeatTimer = setInterval(
      () => this._heartbeat(),
      this.config.timers.heartbeat);

    return this.join();
  }

  /**
   * Join the cluster
   *
   * @param {number} attempts
   */
  async join (attempts = 1) {
    if (this.ready) {
      debugCluster('join - already in. skipping');
      return;
    }

    debugCluster(`join - attempt: ${attempts}`);
    await this.discover();

    const promises = [];

    for (const node of this.pool.values) {
      promises.push(this._remoteSub(node));
    }

    await Bluebird.all(promises);
    await this.state.syncAll();

    if (this.pool.size + 1 >= this.config.minimumNodes) {
      debugCluster('cluster node is ready');
      this.ready = true;
      return this.broadcast('cluster:ready', this);
    }

    // did not join or corum not reached, retry
    if (attempts >= this.config.retryJoin) {
      return;
    }

    await Bluebird.delay(this.config.timers.joinAttemptInterval);

    return this.join(attempts + 1);
  }

  /**
   * Processes incoming sync request from other node
   *
   * @param {object} data
   * @returns {*}
   * @private
   */
  async sync (data) {
    if (data.event !== 'state') {
      debugSync('%o', data);
    }

    switch (data.event) {
      case 'indexCache:add':
        this.kuzzle.storageEngine.indexCache.add({
          collection: data.collection,
          index: data.index,
          notify: false,
          scope: data.scope,
        });
        break;
      case 'indexCache:remove':
        this.kuzzle.storageEngine.indexCache.remove({
          collection: data.collection,
          index: data.index,
          notify: false,
          scope: data.scope,
        });
        break;
      case 'role':
        this.kuzzle.repositories.role.roles.delete(data.id);
        break;
      case 'strategies':
        return this.redis.hgetall('cluster:strategies')
          .then(response => {
            const currentStrategies = new Set(this.kuzzle.pluginsManager.listStrategies());
            const strategies = response || {};

            for (const name of Object.keys(strategies)) {
              currentStrategies.delete(name);
              const payload = JSON.parse(strategies[name]);

              debugCluster('strategy:add: %s, %o', name, payload.strategy);

              try {
                this.kuzzle.pluginsManager.registerStrategy(
                  payload.plugin,
                  name,
                  payload.strategy);
              }
              catch (e) {
                // log & discard
                this.kuzzle.log.error(`Plugin ${payload.plugin} - tried to add strategy "${name}": ${e.message}`);
              }
            }
            // delete strategies
            for (const name of currentStrategies) {
              debugCluster('strategy:del: %s', name);
              const strategy = this.kuzzle.pluginsManager.getStrategy(name);

              if (strategy) {
                this.kuzzle.pluginsManager.unregisterStrategy(
                  strategy.owner,
                  name);
              }
            }
          });
      case 'state':
        return this.state.sync(data);
      case 'state:all':
        return this.state.syncAll(data);
      case 'state:reset':
        return this.kuzzle.cluster.reset();
      case 'validators':
        return this.kuzzle.validation.curateSpecification();
      default:
        throw new KuzzleInternalError(`Unknown sync data received: ${JSON.stringify(data, undefined, 2)}`);
    }
  }

  toJSON () {
    return {
      pub: this.sockets.pub.href,
      ready: this.ready,
      router: this.sockets.router.href,
      uuid: this.kuzzle.cluster.uuid,
    };
  }

  /**
   * Add a cluster node to the list of known siblings
   *
   * @param {object} node
   * @private
   */
  _addNode (node) {
    if (node.pub === this.sockets.pub.href) {
      return;
    }

    this.kuzzle.log.info(`adding node ${JSON.stringify(node)}`);

    if (this.pool.has(node.pub)) {
      this.kuzzle.log.warn(`[_addNode] node already known ${JSON.stringify(node)}`);
      return;
    }

    this.sockets.sub.connect(node.pub);
    this.pool.set(node.pub, node);
    this._onHeartbeat(node);
  }

  async _heartbeat () {
    await this.broadcast('cluster:heartbeat', this);

    const members = await this.redis.smembers('cluster:discovery');
    let found = false;

    for (const serialized of members) {
      const member = JSON.parse(serialized);

      if (member.router === this.sockets.router.href) {
        found = true;
        break;
      }
    }

    if (!found) {
      return this.join();
    }
  }

  /**
   * Called on heartbeat reception from another node
   *
   * @param {object} node
   * @private
   */
  _onHeartbeat (remoteNode) {
    const node = this.pool.get(remoteNode.pub);

    if (!node) {
      return this._remoteJoin(remoteNode);
    }

    clearTimeout(node.heartbeat);

    node.heartbeat = setTimeout(() => {
      this.kuzzle.log.warn(`[cluster] no heartbeat received in time for ${node.pub}. removing node`);
      this._removeNode(node.pub);

      // send a rejoin request to lost node in case this is a temp issue (overload/network congestion..)
      this._remoteJoin(remoteNode);
    }, this.config.timers.heartbeat * 2);
  }

  /**
   * 1 to 1 message received
   *
   * @param {Buffer} envelope
   * @param {buffer} buffer
   * @private
   */
  async _onRouterMessage (envelope, buffer) {
    const [action, data] = JSON.parse(buffer);

    debugCluster('[router][%s] %o', action, data);

    // called from a client to force current node to subscribe to it
    if (action === 'remoteSub') {
      if (!this.pool.has(data.pub)) {
        this._addNode(data);
      }

      return sendPromise(this.sockets.router, [
        envelope,
        JSON.stringify(['remoteSub', true])
      ]);
    }

    // called from a client to force rejoin, for instance if a heartbeat is
    // received back
    if (action === 'remoteJoin') {
      this.ready = false;

      await sendPromise(this.sockets.router, [
        envelope,
        JSON.stringify(['remoteJoin', true])
      ]);

      return this.join();
    }

    throw new KuzzleInternalError(`Unknown router action "${action}`);
  }

  /**
   * Broadcasted (pub/sub) message received
   *
   * @param {Buffer} buffer
   * @private
   */
  async _onSubMessage (buffer) {
    const [room, data] = JSON.parse(buffer);

    switch (room) {
      case 'cluster:heartbeat':
        debugCluster('[sub][%s] %o', room, data);
        this._onHeartbeat(data);
        break;
      case 'cluster:notify:document':
        debugNotify('doc %o', data);
        await this.kuzzle.notifier._notifyDocument(
          data.rooms,
          new Request(data.request.data, data.request.options),
          data.scope,
          data.action,
          data.content);
        break;
      case 'cluster:notify:user':
        debugNotify('user %o', data);
        await this.kuzzle.notifier._notifyUser(
          data.room,
          new Request(data.request.data, data.request.options),
          data.scope,
          data.content);
        break;
      case 'cluster:profile:delete':
        debugCluster('[sub][%s] %o', room, data);
        this.kuzzle.repositories.profile.removeFromCache(data.id);
        break;
      case 'cluster:ready':
        debugCluster('[sub][%s] %o', room, data);

        if (data.pub !== this.kuzzle.cluster.uuid && !this.pool.has(data.pub)) {
          // an unknown node is marked as ready, we are not anymore
          this.kuzzle.log.warn(`[cluster] unknown node ready: ${data.pub}`);

          this.ready = false;
          this._addNode(data);
          return Bluebird.delay(500).then(() => this.join());
        }

        this.pool.get(data.pub).ready = true;
        break;
      case 'cluster:remove':
        debugCluster('[sub][%s] %o', room, data);
        this._removeNode(data.pub);
        break;
      case 'cluster:admin:shutdown':
        debugCluster('[sub][%s] %o', room, data);
        process.kill(process.pid, 'SIGTERM');
        break;
      case 'cluster:sync':
        debugCluster('[sub][%s] %o', room, data);
        await this.sync(data);
        break;
    }
  }

  /**
   * ask {node} to rejoin the cluster
   *
   * @param {node} node
   * @returns {Promise}
   * @private
   */
  async _remoteJoin (node) {
    const socket = zeromq.socket('dealer');
    socket.connect(node.router);

    const deferred = new Deferred();

    socket.on('message', buffer => {
      const [action] = JSON.parse(buffer);

      if (action === 'remoteJoin') {
        socket.close();
        this.discover();
        deferred.resolve();
      }
    });

    await sendPromise(socket, JSON.stringify(['remoteJoin', this]));

    return deferred.promise.timeout(this.config.timers.discoverTimeout)
      .catch(e => {
        // we cannot handle the error and do not want to throw. Just log.
        this.kuzzle.log.error(`_remoteJoin: timeout or unhandled exception ${JSON.stringify(e)}`);
      });
  }

  /**
   * ask {node} to subscribe to us
   *
   * @param {node} node
   * @private
   */
  async _remoteSub (node) {
    const socket = zeromq.socket('dealer');
    socket.connect(node.router);

    const deferred = new Deferred();

    socket.on('message', buffer => {
      const
        [action] = JSON.parse(buffer);

      if (action === 'remoteSub') {
        socket.close();
        deferred.resolve();
      }
    });

    await sendPromise(socket, JSON.stringify(['remoteSub', this ]));

    return deferred.promise.timeout(this.config.timers.discoverTimeout)
      .catch(e => {
        if (e instanceof Bluebird.TimeoutError) {
          return this._removeNode(node.pub);
        }
        throw e;
      });
  }

  /**
   * Removes a sibling node from the pool
   *
   * @param {string} nodePub
   * @private
   */
  async _removeNode (nodePub) {
    debugCluster(`[_removeNode] ${nodePub}`);
    const node = this.pool.get(nodePub);

    if (!node) {
      return;
    }

    clearTimeout(node.heartbeat);
    this.sockets.sub.disconnect(node.pub);
    this.pool.delete(nodePub);

    await this.kuzzle.cluster.cleanNode(node);
    await this.state.syncAll({});

    if (this.pool.size + 1 < this.config.minimumNodes) {
      this.kuzzle.log.warn('[cluster] not enough nodes to run. killing myself');
      this.ready = false;
      await this.broadcast('cluster:remove', this);
      return this.join();
    }
  }
}

function sendPromise (socket, payload) {
  return new Bluebird(resolve => socket.send(payload, 0, resolve));
}

module.exports = Node;
