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

import { moduleFor, test } from 'ember-qunit';
import Ember from 'ember';

moduleFor('route:yarn-queues', 'Unit | Route | yarn queues', {
  unit: true
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
  assert.ok(route.afterModel);
  assert.ok(route.unloadAll);
});

test('Test getting yarn queues', function(assert) {
	let response = {
	  "scheduler": {
	    "schedulerInfo": {
	      "capacity": 100,
	      "maxCapacity": 100,
	      "queueName": "root",
	      "queues": {
	        "queue": [
	          {
	            "absoluteCapacity": 10.5,
	            "absoluteMaxCapacity": 50,
	            "absoluteUsedCapacity": 0,
	            "capacity": 10.5,
	            "maxCapacity": 50,
	            "numApplications": 0,
	            "queueName": "a",
	            "queues": {
	              "queue": [
	                {
	                  "absoluteCapacity": 3.15,
	                  "absoluteMaxCapacity": 25,
	                  "absoluteUsedCapacity": 0,
	                  "capacity": 30.000002,
	                  "maxCapacity": 50,
	                  "numApplications": 0,
	                  "queueName": "a1",
	                  "queues": {
	                    "queue": [
	                      {
	                        "absoluteCapacity": 2.6775,
	                        "absoluteMaxCapacity": 25,
	                        "absoluteUsedCapacity": 0,
	                        "capacity": 85,
	                        "maxActiveApplications": 1,
	                        "maxActiveApplicationsPerUser": 1,
	                        "maxApplications": 267,
	                        "maxApplicationsPerUser": 267,
	                        "maxCapacity": 100,
	                        "numActiveApplications": 0,
	                        "numApplications": 0,
	                        "numContainers": 0,
	                        "numPendingApplications": 0,
	                        "queueName": "a1a",
	                        "resourcesUsed": {
	                          "memory": 0,
	                          "vCores": 0
	                        },
	                        "state": "RUNNING",
	                        "type": "capacitySchedulerLeafQueueInfo",
	                        "usedCapacity": 0,
	                        "usedResources": "<memory:0, vCores:0>",
	                        "userLimit": 100,
	                        "userLimitFactor": 1,
	                        "users": null
	                      },
	                      {
	                        "absoluteCapacity": 0.47250003,
	                        "absoluteMaxCapacity": 25,
	                        "absoluteUsedCapacity": 0,
	                        "capacity": 15.000001,
	                        "maxActiveApplications": 1,
	                        "maxActiveApplicationsPerUser": 1,
	                        "maxApplications": 47,
	                        "maxApplicationsPerUser": 47,
	                        "maxCapacity": 100,
	                        "numActiveApplications": 0,
	                        "numApplications": 0,
	                        "numContainers": 0,
	                        "numPendingApplications": 0,
	                        "queueName": "a1b",
	                        "resourcesUsed": {
	                          "memory": 0,
	                          "vCores": 0
	                        },
	                        "state": "RUNNING",
	                        "type": "capacitySchedulerLeafQueueInfo",
	                        "usedCapacity": 0,
	                        "usedResources": "<memory:0, vCores:0>",
	                        "userLimit": 100,
	                        "userLimitFactor": 1,
	                        "users": null
	                      }
	                    ]
	                  },
	                  "resourcesUsed": {
	                    "memory": 0,
	                    "vCores": 0
	                  },
	                  "state": "RUNNING",
	                  "usedCapacity": 0,
	                  "usedResources": "<memory:0, vCores:0>"
	                },
	                {
	                  "absoluteCapacity": 7.35,
	                  "absoluteMaxCapacity": 50,
	                  "absoluteUsedCapacity": 0,
	                  "capacity": 70,
	                  "maxActiveApplications": 1,
	                  "maxActiveApplicationsPerUser": 100,
	                  "maxApplications": 735,
	                  "maxApplicationsPerUser": 73500,
	                  "maxCapacity": 100,
	                  "numActiveApplications": 0,
	                  "numApplications": 0,
	                  "numContainers": 0,
	                  "numPendingApplications": 0,
	                  "queueName": "a2",
	                  "resourcesUsed": {
	                    "memory": 0,
	                    "vCores": 0
	                  },
	                  "state": "RUNNING",
	                  "type": "capacitySchedulerLeafQueueInfo",
	                  "usedCapacity": 0,
	                  "usedResources": "<memory:0, vCores:0>",
	                  "userLimit": 100,
	                  "userLimitFactor": 100,
	                  "users": null
	                }
	              ]
	            },
	            "resourcesUsed": {
	              "memory": 0,
	              "vCores": 0
	            },
	            "state": "RUNNING",
	            "usedCapacity": 0,
	            "usedResources": "<memory:0, vCores:0>"
	          },
	          {
	            "absoluteCapacity": 89.5,
	            "absoluteMaxCapacity": 100,
	            "absoluteUsedCapacity": 0,
	            "capacity": 89.5,
	            "maxCapacity": 100,
	            "numApplications": 2,
	            "queueName": "b",
	            "queues": {
	              "queue": [
	                {
	                  "absoluteCapacity": 53.7,
	                  "absoluteMaxCapacity": 100,
	                  "absoluteUsedCapacity": 0,
	                  "capacity": 60.000004,
	                  "maxActiveApplications": 1,
	                  "maxActiveApplicationsPerUser": 100,
	                  "maxApplications": 5370,
	                  "maxApplicationsPerUser": 537000,
	                  "maxCapacity": 100,
	                  "numActiveApplications": 1,
	                  "numApplications": 2,
	                  "numContainers": 0,
	                  "numPendingApplications": 1,
	                  "queueName": "b1",
	                  "resourcesUsed": {
	                    "memory": 0,
	                    "vCores": 0
	                  },
	                  "state": "RUNNING",
	                  "type": "capacitySchedulerLeafQueueInfo",
	                  "usedCapacity": 0,
	                  "usedResources": "<memory:0, vCores:0>",
	                  "userLimit": 100,
	                  "userLimitFactor": 100,
	                  "users": {
	                    "user": [
	                      {
	                        "numActiveApplications": 0,
	                        "numPendingApplications": 1,
	                        "resourcesUsed": {
	                          "memory": 0,
	                          "vCores": 0
	                        },
	                        "username": "user2"
	                      },
	                      {
	                        "numActiveApplications": 1,
	                        "numPendingApplications": 0,
	                        "resourcesUsed": {
	                          "memory": 0,
	                          "vCores": 0
	                        },
	                        "username": "user1"
	                      }
	                    ]
	                  }
	                },
	                {
	                  "absoluteCapacity": 35.3525,
	                  "absoluteMaxCapacity": 100,
	                  "absoluteUsedCapacity": 0,
	                  "capacity": 39.5,
	                  "maxActiveApplications": 1,
	                  "maxActiveApplicationsPerUser": 100,
	                  "maxApplications": 3535,
	                  "maxApplicationsPerUser": 353500,
	                  "maxCapacity": 100,
	                  "numActiveApplications": 0,
	                  "numApplications": 0,
	                  "numContainers": 0,
	                  "numPendingApplications": 0,
	                  "queueName": "b2",
	                  "resourcesUsed": {
	                    "memory": 0,
	                    "vCores": 0
	                  },
	                  "state": "RUNNING",
	                  "type": "capacitySchedulerLeafQueueInfo",
	                  "usedCapacity": 0,
	                  "usedResources": "<memory:0, vCores:0>",
	                  "userLimit": 100,
	                  "userLimitFactor": 100,
	                  "users": null
	                },
	                {
	                  "absoluteCapacity": 0.4475,
	                  "absoluteMaxCapacity": 100,
	                  "absoluteUsedCapacity": 0,
	                  "capacity": 0.5,
	                  "maxActiveApplications": 1,
	                  "maxActiveApplicationsPerUser": 100,
	                  "maxApplications": 44,
	                  "maxApplicationsPerUser": 4400,
	                  "maxCapacity": 100,
	                  "numActiveApplications": 0,
	                  "numApplications": 0,
	                  "numContainers": 0,
	                  "numPendingApplications": 0,
	                  "queueName": "b3",
	                  "resourcesUsed": {
	                    "memory": 0,
	                    "vCores": 0
	                  },
	                  "state": "RUNNING",
	                  "type": "capacitySchedulerLeafQueueInfo",
	                  "usedCapacity": 0,
	                  "usedResources": "<memory:0, vCores:0>",
	                  "userLimit": 100,
	                  "userLimitFactor": 100,
	                  "users": null
	                }
	              ]
	            },
	            "resourcesUsed": {
	              "memory": 0,
	              "vCores": 0
	            },
	            "state": "RUNNING",
	            "usedCapacity": 0,
	            "usedResources": "<memory:0, vCores:0>"
	          }
	        ]
	      },
	      "type": "capacityScheduler",
	      "usedCapacity": 0
	    }
	  }
	};
	let store = {
		query: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve(response);
      });
		}
	};
	let route = this.subject();
	route.set('store', store);
	route.model({queue_name: 'a1'}).then(function(model) {
		assert.ok(model);
		assert.ok(model.queues);
		assert.ok(model.selected);
		assert.equal(model.selected, 'a1');
		assert.deepEqual(model.queues, response);
	});
});