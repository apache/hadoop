/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Ember from 'ember';
import { moduleFor, test } from 'ember-qunit';

moduleFor('service:application-diagnostic', 'Unit | Service | application diagnostic', {
  unit: true,
  beforeEach() {
    this.register('service:hosts', Ember.Service.extend({
      rmWebAddress: 'http://localhost:8088'
    }), { instantiate: false });
    this.register('service:env', Ember.Service.extend({
      app: {
        namespaces: {
          cluster: 'ws/v1/cluster'
        }
      }
    }), { instantiate: false });
  }
});

test('buildCollectUrl encodes issue and app id', function(assert) {
  const service = this.subject();
  const appId = 'application_1789971760389_0001';
  const url = service.buildCollectUrl(appId);

  assert.equal(
    url,
    'http://localhost:8088/ws/v1/cluster/common-issues/collect' +
      '?issueId=application_diagnostic&args=application_1789971760389_0001'
  );
});

test('parseAjaxError extracts RemoteException message', function(assert) {
  const service = this.subject();
  const error = service.parseAjaxError({
    responseJSON: {
      RemoteException: {
        message: 'Error collecting the selected issue data.'
      }
    }
  });

  assert.equal(error.message, 'Error collecting the selected issue data.');
});

test('parseAjaxError handles connection failure', function(assert) {
  const service = this.subject();
  const error = service.parseAjaxError({ status: 0, responseText: '' });

  assert.equal(error.message, 'Not able to connect to the Resource Manager.');
});
