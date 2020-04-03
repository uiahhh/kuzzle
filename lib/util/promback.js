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

const Deferred = require('./deferred');

class Promback {
  constructor (callback = null) {
    this._callback = callback;
    this.deferred = null;
    this.isPromise = this._callback === null;

    if (this.isPromise) {
      this.deferred = new Deferred();
    }
  }

  resolve (result) {
    if (this.isPromise) {
      this.deferred.resolve(result);
    }
    else {
      this._callback(null, result);
    }

    return this.promise;
  }

  reject (error) {
    if (this.isPromise) {
      this.deferred.reject(error);
    }
    else {
      this._callback(error);
    }

    return this.promise;
  }

  get promise () {
    return this.isPromise ? this.deferred.promise : null;
  }
}

module.exports = Promback;
