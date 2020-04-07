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

// Actions performed upon receiving a cluster sync message, all in the same
// place, for an easy to maintain list of synced objects in Kuzzle.
//
// All actions:
//   * are asynchronous and should be awaited
//   * receive a JSON object as their payload
//   * have their own debug category (e.g. cluster:notify:document logs in the
//     "kuzzle:cluster:notify:document" category)

const debug = require('../util/debug');
const errorManager = require('../util/errors');

// Single sync action object
class Action {
  constructor (ctx, action, fn) {
    this.action = action;
    this._fn = fn.bind(ctx);
    this._debug = debug(action);
  }

  get execute () {
    return this._fn;
  }

  get debug () {
    return this._debug;
  }
}

class NodeSync {
  constructor (kuzzle) {
    this.kuzzle = kuzzle;
    this._actions = new Map();

    this._register('cluster:admin:dump', this._onAdminDump);
    this._register('cluster:admin:resetSecurity', this._onAdminResetSecurity);
  }

  _register (action, fn) {
    this._actions.set(this, action, new Action(action, fn));
  }

  async dispatch (buffer) {
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

  // ----------- ACTIONS -----------
  async _onAdminDump (data) {
    return this.kuzzle.janitor.dump(data.suffix);
  }

  async _onAdminResetSecurity (/* Unused: data */) {
    this.kuzzle.repositories.profile.clearCache();
    this.kuzzle.repositories.role.clearCache();
  }
}

module.exports = NodeSync;
