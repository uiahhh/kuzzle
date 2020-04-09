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
const { Request } = require('kuzzle-common-objects');
const RedisStateManager = require('./redis/manager');
const zeromq = require('zeromq/v5-compat');
const ZSocket = require('./zsocket');
const Deferred = require('../util/deferred');


// Single sync action object
class Action {
  constructor (ctx, action, fn) {
    this.action = action;
    this._fn = fn.bind(ctx);
    this._debug = debug(`cluster:${action}`);
  }

  get execute () {
    return this._fn;
  }

  get debug () {
    return this._debug;
  }
}

class Node {
  constructor (kuzzle) {
    this.kuzzle = kuzzle;
    this.config = kuzzle.config.cluster;
    this.actions = this.registerActions();

    this.ready = false;

    this.sockets = {
      pub: new ZSocket('pub', this.config.bindings.pub, 7511),
      router: new ZSocket('router', this.config.bindings.router, 7510),
      sub: new ZSocket('sub'),
    };

    this.sockets.router.on('message', this._onRouterMessage.bind(this));
    this.sockets.sub.on('message', this.dispatchAction.bind(this));
    this.sockets.sub.subscribe('');

    // active node pool
    this.pool = new Map();

    this.state = new RedisStateManager(this);

    this.heartbeatTimer = null;
  }

  registerActions () {
    const actions = new Map();
    const register = (action, fn) => actions.set(
      this,
      action,
      new Action(action, fn));

    register('admin:dump', this._onAdminDump);
    register('admin:resetSecurity', this._onAdminResetSecurity);
    register('admin:shutdown', this._onAdminShutdown);
    register('auth:strategies', this._onAuthStrategies);
    register('cluster:heartbeat', this._onHeartbeat);
    register('cluster:ready', this._onReady);
    register('cluster:remove', this.onRemove);
    register('indexCache:add', this._onIndexCacheAdd);
    register('indexCache:remove', this._onIndexCacheRemove);
    register('notify:document', this._onNotifyDocument);
    register('notify:user', this._onNotifyUser);
    register('profile:update', this._onProfileUpdate);
    register('role:update', this._onRoleUpdate);
    register('state:all', this._onStateAll);
    register('state:realtime', this._onStateRealtime);
    register('state:reet', this._onStateReset);
    register('validators:update', this._onValidatorsUpdate);

    return actions;
  }

  async dispatchAction (buffer) {
    const [room, data] = JSON.parse(buffer);
    const action = this._actions.get(room);

    if (!action) {
      // Will be uncaught, making Kuzzle crash in dev environments, and
      // log an error otherwise.
      throw new Error(`Unknown sync message destination: "${room}"`);
    }

    action.debug('received sync message: %o', data);

    try {
      await action.execute(data);
    }
    catch (error) {
      this.kuzzle.log.error(error);
    }
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
  async broadcast (event, data = {}) {
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
      return this.broadcast('state:all');
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

    const joinedCluster = members
      .some(m => JSON.parse(m).router === this.sockets.router.href);

    if (!joinedCluster) {
      return this.join();
    }
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

    try {
      // called from a client to force current node to subscribe to it
      if (action === 'remoteSub') {
        if (!this.pool.has(data.pub)) {
          this._addNode(data);
        }

        await sendPromise(this.sockets.router, [
          envelope,
          JSON.stringify(['remoteSub', true])
        ]);
      }
      // called from a client to force rejoin, for instance if a heartbeat is
      // received back
      else if (action === 'remoteJoin') {
        this.ready = false;

        await sendPromise(this.sockets.router, [
          envelope,
          JSON.stringify(['remoteJoin', true])
        ]);

        await this.join();
      }
      else {
        this.kuzzle.log.error(`Unknown router action "${action}`);
      }
    }
    catch (e) {
      this.kuzzle.log.error(`Error occured when processing the envelope: ${envelope}\n${e.stack}`);
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

    socket.on('message', async buffer => {
      const [action] = JSON.parse(buffer);

      if (action === 'remoteJoin') {
        socket.close();
        await this.discover();
        deferred.resolve();
      }
    });

    await sendPromise(socket, JSON.stringify(['remoteJoin', this]));

    try {
      await deferred.promise.timeout(this.config.timers.discoverTimeout);
    }
    catch(e) {
      // we cannot handle the error and do not want to throw. Just log.
      if (e instanceof Bluebird.TimeoutError) {
        this.kuzzle.log.error('_remoteJoin: timeout');
      }
      else {
        this.kuzzle.log.error(`_remoteJoin: ${e.stack}`);
      }
    }
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
      const [action] = JSON.parse(buffer);

      if (action === 'remoteSub') {
        socket.close();
        deferred.resolve();
      }
    });

    await sendPromise(socket, JSON.stringify(['remoteSub', this]));

    try {
      await deferred.promise.timeout(this.config.timers.discoverTimeout);
    }
    catch(e) {
      if (e instanceof Bluebird.TimeoutError) {
        return this._removeNode(node.pub);
      }

      throw e;
    }
  }

  /**
   * Removes a sibling node from the pool
   *
   * @param {string} nodePub
   * @private
   */
  async _removeNode (nodePub) {
    debugCluster('Removing node: %s', nodePub);
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

  // ----------- SYNC ACTIONS -----------
  async _onAdminDump (data) {
    return this.kuzzle.janitor.dump(data.suffix);
  }

  async _onAdminResetSecurity (/* Unused: data */) {
    this.kuzzle.repositories.profile.clearCache();
    this.kuzzle.repositories.role.clearCache();
  }

  /**
   * Called on heartbeat reception from another node
   */
  async _onHeartbeat (data) {
    const node = this.pool.get(data.pub);

    if (!node) {
      return this._remoteJoin(data);
    }

    clearTimeout(node.heartbeat);

    node.heartbeat = setTimeout(
      () => {
        this.kuzzle.log.warn(`[cluster] no heartbeat received in time for ${node.pub}. removing node`);
        this._removeNode(node.pub);

        // send a rejoin request to lost node in case this is a temp issue
        // (overload/network congestion..)
        this._remoteJoin(data);
      },
      this.config.timers.heartbeat * 2);
  }

  async _onNotifyDocument (data) {
    return this.kuzzle.notifier._notifyDocument(
      data.rooms,
      new Request(data.request.data, data.request.options),
      data.scope,
      data.action,
      data.content);
  }

  async _onNotifyUser (data) {
    return this.kuzzle.notifier._notifyUser(
      data.room,
      new Request(data.request.data, data.request.options),
      data.scope,
      data.content);
  }

  async _onProfileUpdate (data) {
    this.kuzzle.repositories.profile.removeFromCache(data.id);
  }

  async _onRoleUpdate (data) {
    this.kuzzle.repositories.role.removeFromCache(data.id);
  }

  async _onReady (data) {
    if (data.pub !== this.kuzzle.cluster.uuid && !this.pool.has(data.pub)) {
      // an unknown node is marked as ready, we are not anymore
      this.kuzzle.log.warn(`[cluster] unknown node ready: ${data.pub}`);

      this.ready = false;
      this._addNode(data);
      await Bluebird.delay(500);

      return this.join();
    }

    this.pool.get(data.pub).ready = true;
  }

  async _onRemove (data) {
    return this._removeNode(data.pub);
  }

  async _onAdminShutdown (/* Unused: data */) {
    process.kill(process.pid, 'SIGTERM');
  }

  async _onIndexCacheAdd (data) {
    return this.kuzzle.storageEngine.indexCache.add({
      collection: data.collection,
      index: data.index,
      notify: false,
      scope: data.scope,
    });
  }

  async _onIndexCacheRemove (data) {
    return this.kuzzle.storageEngine.indexCache.remove({
      collection: data.collection,
      index: data.index,
      notify: false,
      scope: data.scope,
    });
  }

  async _onAuthStrategies (/* Unused: data */) {
    const currentStrategies = new Set(this.kuzzle.pluginsManager.listStrategies());
    const response = this.redis.hgetall('cluster:strategies');
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
      debugCluster('Removing authentication strategy: "%s"', name);
      const strategy = this.kuzzle.pluginsManager.getStrategy(name);

      if (strategy) {
        this.kuzzle.pluginsManager.unregisterStrategy(strategy.owner, name);
      }
    }
  }

  async _onStateRealtime (data) {
    return this.state.sync(data);
  }

  async _onStateAll (/* Unused: data */) {
    await this.state.syncAll();

    return this._onAuthStrategies();
  }

  async _onStateReset (/* Unused: data */) {
    return this.kuzzle.cluster.reset();
  }

  async _onValidatorsUpdate (/* Unused: data */) {
    return this.kuzzle.validation.curateSpecification();
  }
}

function sendPromise (socket, payload) {
  return new Bluebird(resolve => socket.send(payload, 0, resolve));
}

module.exports = Node;
