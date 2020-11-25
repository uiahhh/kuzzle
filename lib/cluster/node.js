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

const assert = require('assert');

const getIP = require('ip');
const Bluebird = require('bluebird');

const debug = require('../util/debug')('kuzzle:cluster');
const nameGenerator = require('../util/name-generator');
const Mutex = require('../util/mutex');
const ClusterPublisher = require('./publisher');
const ClusterSubscriber = require('./subscriber');
const ClusterState = require('./state');
const ClusterCommand = require('./command');

const REDIS_PREFIX = '{cluster/node}/';

// Handles the node logic: discovery, eviction, heartbeat, ...
// Dependencies: core:cache module must be started
class ClusterNode {
  constructor (kuzzle) {
    this.kuzzle = kuzzle;
    this.config = kuzzle.config.cluster;
    this.heartbeatDelay = this.config.heartbeat;

    checkConfig(this.config);

    this.ip = getIP.address('public', this.config.ipv6 ? 'ipv6' : 'ipv4');

    this.nodeId = null;
    this.nodeIdKey = null;
    this.heartbeatTimer = null;

    this.publisher = new ClusterPublisher(this);
    this.fullState = new ClusterState(this.kuzzle);
    this.command = new ClusterCommand(this);

    /**
     * Links remote node IDs with their subscriber counterpart
     * @type {Map.<string, ClusterSubscriber>}
     */
    this.remoteNodes = new Map();
  }

  get syncAddress () {
    return `tcp://${this.ip}:${this.config.ports.sync}`;
  }

  async init () {
    // The publisher needs to be created and initialized before the handshake:
    // other nodes we'll connect to during the handshake will start to subscribe
    // to this node right away
    await this.publisher.init();

    // This also needs to be started before the handshake, as this class handles
    // direct requests to other nodes (needed to request for the full state
    // and to introduce oneself to other nodes)
    await this.command.init();

    this.kuzzle.on('kuzzle:shutdown', () => this.shutdown());

    await this.handshake();

    this.registerEvents();

    return this.nodeId;
  }

  /**
   * Shutdown event: clears all timers, sends a termination status to other
   * nodes, and removes entries from the cache
   */
  async shutdown () {
    debug('[%s] Removing myself from the cluster...', this.nodeId);
    clearInterval(this.heartbeatTimer);
    await this.kuzzle.ask('core:cache:internal:del', this.nodeIdKey);

    for (const subscriber of this.remoteNodes.values()) {
      subscriber.dispose();
    }

    await this.publisher.send('NodeShutdown', { nodeId: this.nodeId });
    await this.publisher.dispose();
    this.command.dispose();
  }

  /**
   * Generates and reserves a unique ID for this node instance.
   * Makes sure that the ID is not already taken by another node instance.
   *
   * @return {void}
   */
  async generateId () {
    let reserved;

    do {
      this.nodeId = nameGenerator();
      this.nodeIdKey = `${REDIS_PREFIX}${this.nodeId}`;

      reserved = await this.kuzzle.ask(
        'core:cache:internal:store',
        this.nodeIdKey,
        this.ip,
        { onlyIfNew: true, ttl: this.heartbeatDelay * 1.5 });
    } while (!reserved);

    this.heartbeatTimer = setInterval(
      async () => {
        await this.kuzzle.ask(
          'core:cache:internal:pexpire',
          this.nodeIdKey,
          this.heartbeatDelay * 1.5);

        await this.publisher.send('Heartbeat', { address: this.syncAddress });
      },
      this.heartbeatDelay);
  }

  /**
   * Adds a new remote node, and subscribes to it.
   * @param {string} id            - remote node ID
   * @param {string} ip            - remote node IP address
   * @param {number} lastMessageId - remote node last message ID
   * @return {boolean} false if the node was already known, true otherwise
   */
  async addNode (id, ip, lastMessageId) {
    if (this.remoteNodes.has(id)) {
      return false;
    }

    const subscriber = new ClusterSubscriber(this, id, `tcp://${ip}:${this.config.ports.sync}`);

    this.remoteNodes.set(id, subscriber);
    await subscriber.init();
    await subscriber.sync(lastMessageId);

    return true;
  }

  /**
   * Evicts a remote from the list
   * @param {string}  nodeId - remote node ID
   * @param {Object}  [options]
   * @param {boolean} [options.broadcast] - broadcast the eviction to the cluster
   * @param {string}  [options.reason] - reason of eviction
   */
  async evictNode (nodeId, { broadcast, reason = '' }) {
    const subscriber = this.remoteNodes.get(nodeId);

    if (!subscriber) {
      return;
    }

    this.kuzzle.log.warn(`[CLUSTER] Node "${nodeId}" evicted. Reason: ${reason}`);
    this.remoteNodes.delete(nodeId);
    subscriber.dispose();

    if (broadcast) {
      await this.publisher.send('NodeEvicted', {
        evictor: this.nodeId,
        nodeId,
        reason,
      });
    }
  }

  /**
   * Discovers other active nodes from the cluster and, if other nodes exist,
   * starts a handshake procedure to sync this node and to make it able to
   * handle new client requests
   *
   * @return {void}
   */
  async handshake () {
    const mutex = new Mutex(this.kuzzle, 'clusterHandshake', {
      timeout: 60000,
    });

    await mutex.lock();

    try {
      // Create this node ID until AFTER the handshake mutex is actually locked,
      // to prevent race conditions (other nodes attempting to connect to this
      // node while it's still initializing)
      await this.generateId();

      let retried = false;
      let fullState = null;
      let nodes;

      do {
        nodes = await this.listRemoteNodes();

        // No other nodes detected = no handshake required
        if (nodes.length === 0) {
          return;
        }

        // Subscribe to remote nodes and start buffering sync messages
        await Bluebird.map(nodes, ([id, ip]) => {
          const subscriber = new ClusterSubscriber(this, id, `tcp://${ip}:${this.config.ports.sync}`);
          this.remoteNodes.set(id, subscriber);
          return subscriber.init();
        });

        fullState = await this.command.getFullState(nodes);

        // Uh oh... no node was able to give us the full state.
        // We must retry later, to check if the redis keys have expired. If they
        // are still there and we still aren't able to fetch a full state, this
        // means we're probably facing a network split, and we must then shut
        // down.
        if (fullState === null) {
          if (retried) {
            this.kuzzle.log.error('[FATAL] Could not connect to discovered cluster nodes (network split detected). Shutting down.');
            this.kuzzle.shutdown();
            return;
          }

          // Disposes all subscribers
          for (const subscriber of this.remoteNodes.values()) {
            subscriber.dispose();
          }
          this.remoteNodes.clear();

          // Waits for a redis heartbeat round
          retried = true;
          const retryDelay = this.heartbeatDelay * 1.5;
          this.kuzzle.log.warn(`Unable to connect to discovered cluster nodes. Retrying in ${retryDelay}ms...`);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        }
      }
      while (fullState === null);

      await this.fullState.loadFullState(fullState);

      const handshakeResponses = await this.command.broadcastHandshake(nodes);

      // Update subscribers: start synchronizing, or unsubscribes from nodes who
      // didn't respond
      for (const [nodeId, handshakeData] of Object.entries(handshakeResponses)) {
        const subscriber = this.remoteNodes.get(nodeId);
        if (handshakeData === null) {
          subscriber.dispose();
          this.remoteNodes.delete(nodeId);
        }
        else {
          subscriber.sync(handshakeData.lastMessageId);
          this.kuzzle.log.info(`Successfully completed the handshake with node ${nodeId}`);
        }
      }

      this.kuzzle.log.info('Successfully joined the cluster.');
    }
    finally {
      mutex.unlock();
    }
  }

  /**
   * Retrieves the list of other nodes from Redis
   * @return {Array.<Array>} key: nodeId, value: IP address
   */
  async listRemoteNodes () {
    const result = [];

    let keys = await this.kuzzle.ask(
      'core:cache:internal:searchKeys',
      `${REDIS_PREFIX}*`);

    keys = keys.filter(nodeIdKey => nodeIdKey !== this.nodeIdKey);

    if (keys.length === 0) {
      return result;
    }

    const values = await this.kuzzle.ask('core:cache:internal:mget', keys);

    for (let i = 0; i < keys.length; i++) {
      // filter keys that might have expired between the key search and their
      // values retrieval
      if (values[i] !== null) {
        result.push([keys[i].replace(REDIS_PREFIX, ''), values[i]]);
      }
    }

    return result;
  }

  /**
   * Starts listening to API events, to trigger sync messages.
   * @return {void}
   */
  registerEvents () {
    /**
     * Removes a room from the full state, and only for this node.
     * Removes the room from Koncorde if, and only if, no other node uses it.
     *
     * @param  {string} roomId
     * @return {void}
     */
    this.kuzzle.onAsk(
      'cluster:realtime:room:remove',
      roomId => this.onRealtimeRoomRemoval(roomId));


    this.kuzzle.onPipe(
      'core:realtime:user:subscribe:after',
      subscription => this.onNewSubscription(subscription));

    this.kuzzle.onPipe(
      'core:realtime:user:unsubscribe:after',
      roomId => this.onUnsubscription(roomId));
  }

  /**
   * Triggered on a new realtime subscription: sync other nodes and updates the
   * full state.
   *
   * @param  {Object} subscription
   * @return {void}
   */
  async onNewSubscription (subscription) {
    const { roomId, index, collection, filters } = subscription;

    if (this.subscription.created) {
      const roomMessageId = await this.publisher.sendNewRealtimeRoom (
        roomId,
        index,
        collection,
        filters);

      this.state.addRealtimeRoom(roomId, index, collection, filters, {
        lastMessageId: roomMessageId,
        nodeId: this.nodeId,
        subscribers: 0,
      });
    }

    const subMessageId = await this.publisher.sendSubscription(roomId);
    this.state.addRealtimeSubscription(roomId, this.nodeId, subMessageId);
  }

  /**
   * Triggered when a realtime room is removed from this node.
   *
   * @param  {string} roomId
   * @return {void}
   */
  async onRealtimeRoomRemoval (roomId) {
    this.state.removeRealtimeRoom(roomId, this.nodeId);
    await this.publisher.sendRealtimeRoomRemoval(roomId);
  }

  /**
   * Triggered when a user unsubscribes from a room
   *
   * @param  {string} roomId
   * @return {void}
   */
  async onUnsubscription (roomId) {
    const messageId = this.publisher.sendUnsubscription(roomId);
    this.state.removeRealtimeSubscription(roomId, this.nodeId, messageId);
  }
}

function checkConfig (config) {
  for (const prop of ['heartbeat', 'joinTimeout', 'minimumNodes']) {
    assert(
      typeof config[prop] === 'number' && config[prop] > 0,
      `[FATAL] kuzzlerc.cluster.${prop}: value must be a number greater than 0`);
  }

  for (const prop of ['command', 'sync']) {
    assert(
      typeof config.ports[prop] === 'number' && config.ports[prop] > 0,
      `[FATAL] kuzzlerc.cluster.ports.${prop}: value must be a number greater than 0`);
  }

  assert(typeof config.ipv6 === 'boolean', '[FATAL] kuzzlerc.cluster.ipv6: boolean expected');
}

module.exports = ClusterNode;