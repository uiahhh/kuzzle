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

const url = require('url');
const Bluebird = require('bluebird');
const ip = require('ip');
const zeromq = require('zeromq/v5-compat');

class ZSocket {
  constructor(name, config = null, port = -1) {
    this.name = name;
    this.href = config ? this._resolveBinding(config, port) : '';
    this.socket = zeromq.socket(name);
  }

  async bind () {
    if (!this.href) {
      return null;
    }

    const _bind = Bluebird.promisify(this.socket.bind, {context: this.socket});

    return _bind(this.href);
  }

  get connect () {
    return this.socket.connect;
  }

  get on () {
    return this.socket.on;
  }

  get subscribe () {
    return this.socket.subscribe;
  }

  get send () {
    return this.socket.send;
  }

  get disconnect () {
    return this.socket.disconnect;
  }

  /**
   *
   * @param {String} hostConfig The host representation as string
   *                            e.g. tcp://[eth0:ipv6]:9876
   * @param {integer} defaultPort Default port to use if none found from the
   *                              config
   * @returns URL
   * @private
   */
  _resolveBinding (hostConfig, defaultPort) {
    const {
      host,
      hostname,
      port = defaultPort,
      protocol = 'tcp'
    } = url.parse(hostConfig, false, true);

    let target = hostname;

    if (/^\[.+\]/.test(host)) {
      const tmp = host.split(':');
      const family = tmp[1] || 'ipv4';

      if (tmp[0] === '_site_') {
        tmp[0] = 'public';
      }

      target = ip.address(tmp[0], family.toLowerCase());
    }


    return url
      .parse(`${protocol}://${target}:${port}`)
      .href;
  }
}

module.exports = ZSocket;
