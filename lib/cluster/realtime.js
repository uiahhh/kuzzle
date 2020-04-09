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
const {Request} = require('kuzzle-common-objects');
const errorManager = require('../util/errors');

// Cluster-wide realtime methods
class ClusterRealtime {
  constructor (kuzzle) {
    this.kuzzle = kuzzle;

    this._rooms = {
      // Map.<room id, room>
      flat: new Map(),
      // Map.<index, Map.<collection, Set.<room id> > >
      tree: new Map()
    };
  }

  hasRoom (roomId) {
    const room = this._rooms.flat.get(roomId);

    return room && room.count > 1;
  }

  /**
   * Return a cluster-wide count of subscriptions made on a given room ID
   * @param {String} roomId
   */
  async countSubscriptions (roomId) {
    if (!this._rooms.flat.has(roomId)) {
      // no room found. May be normal but can also be due to cluster replication
      // time
      await Bluebird.delay(this.config.timers.waitForMissingRooms);

      if (!this._rooms.flat.has(roomId)) {
        throw errorManager.get('core', 'realtime', 'room_not_found', roomId);
      }
    }

    return {count: this._rooms.flat.get(roomId).count};
  }

  /**
   * Return a cluster-wide list of subscriptions
   * @param {Request} request
   * @private
   */
  async listSubscriptions (request) {
    const list = {};

    for (const [index, collectionTree] of this._rooms.tree.entries()) {
      for (const [collection, rooms] of collectionTree.entries()) {
        const rq = new Request({
          action: 'search',
          collection,
          controller: 'document',
          index,
        });

        if (await request.context.user.isActionAllowed(rq)) {
          if (!list[index]) {
            list[index] = {};
          }

          list[index][collection] = {};

          for (const id of rooms) {
            list[index][collection][id] = this._rooms.flat.get(id).count;
          }
        }
      }
    }

    return list;
  }

  reset () {
    this._rooms.flat.clear();
    this._rooms.tree.clear();
  }
}

module.exports = ClusterRealtime;
