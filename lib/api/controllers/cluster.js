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

const errorsManager = require('../../util/errors');
const { NativeController } = require('./base');

/**
 * @class ClusterController
 * @param {Kuzzle} kuzzle
 */
class ClusterController extends NativeController {
  constructor (kuzzle) {
    super(kuzzle, [
      'health',
      'reset',
      'status',
    ]);
  }

  async health () {
    this.assertNodeReady();
    return 'ok';
  }

  async reset () {
    this.assertNodeReady();

    await this.kuzzle.node.reset();
    await this.node.broadcast('cluster:sync', {event: 'state:reset'});

    return 'ok';
  }

  async status () {
    this.assertNodeReady();

    return {
      count: 1 + Object.keys(this.kuzzle.node.pool).length,
      current: {
        pub: this.kuzzle.node.config.bindings.pub.href,
        ready: this.kuzzle.node.ready,
        router: this.kuzzle.node.config.bindings.router.href,
      },
      pool: Object.values(this.kuzzle.node.pool).map(node => ({
        pub: node.pub,
        ready: node.ready,
        router: node.router,
      }))
    };
  }

  assertNodeReady () {
    if (!this.kuzzle.node.ready) {
      throw errorsManager.get('cluster', 'node', 'not_ready');
    }
  }
}

module.exports = ClusterController;
