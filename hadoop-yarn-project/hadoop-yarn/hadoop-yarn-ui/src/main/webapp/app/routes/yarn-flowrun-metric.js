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
import ErrorUtils from 'yarn-ui/utils/error-utils';

export default Ember.Route.extend({
  model: function(params) {
    var _this = this;
   var appsArr = [];
   var flowrun = _this.store.queryRecord('yarn-flowrun', {flowrun_uid: params.flowrun_uid,
        metricstoretrieve: params.metric_id}).then(function(value) {
      var apps = _this.store.query('yarn-app-flowrun', {flowrunUid: params.flowrun_uid,
          metricstoretrieve: params.metric_id}).then(function(value) {
        for (var i = 0; i < value.content.length; i++) {
          var tasks = undefined;
          // No need to fetch task or container info for Job counters.
          if (params.metric_id.indexOf("JobCounter") === -1) {
            var entityType = "MAPREDUCE_TASK";
            // CPU and MEMORY are container metrics.
            if (params.metric_id === "CPU" || params.metric_id === "MEMORY") {
              entityType = "YARN_CONTAINER";
            }
            tasks = _this.fetchTasksForEntity(value.content[i]._data.uid, params.metric_id, entityType);
          }
          appsArr.push(Ember.RSVP.hash({id: value.content[i].id, tasks: tasks , metrics: value.content[i]._data.metrics}));
        }
        return Ember.RSVP.all(appsArr);
      });
      return Ember.RSVP.hash({run: value, id: value.id, metrics: value._internalModel._data.metrics, apps: apps});
    });
    return Ember.RSVP.all([Ember.RSVP.hash({flowrun:flowrun, metricId: params.metric_id})]);
  },

  fetchTasksForEntity: function(appUid, metricId, entityType) {
    return this.store.query('yarn-entity', {
      app_uid: appUid,
      metricstoretrieve: metricId,
      entity_type: entityType,
      metricslimit: 100
    }).then(function(value) {
      var tasksArr = [];
      for (var j = 0; j < value.content.length; j++) {
        tasksArr.push(Ember.RSVP.hash({id: value.content[j].id, metrics: value.content[j]._data.metrics}));
      }
      return Ember.RSVP.all(tasksArr);
    });
  },

  getMetricValue: function(metrics) {
    var metricValue = 0;
    if (metrics.length > 0) {
      for (var j in metrics[0].values) {
        metricValue = metrics[0].values[j];
        break;
      }
    }
    return metricValue.toString();
  },

  setupController: function(controller, model) {
    var metricsArr = [];
    var flowRunId = model[0].flowrun.id;
    metricsArr.push([model[0].flowrun.id, this.getMetricValue(model[0].flowrun.metrics)]);
    for (var i = 0; i < model[0].flowrun.apps.length; i++) {
      var appId = flowRunId + '-' + model[0].flowrun.apps[i].id;
      metricsArr.push([appId, this.getMetricValue(model[0].flowrun.apps[i].metrics)]);
      if (model[0].flowrun.apps[i].tasks) {
        for (var j = 0; j < model[0].flowrun.apps[i].tasks.length; j++) {
          var taskId = appId + '-' + model[0].flowrun.apps[i].tasks[j].id;
          metricsArr.push([taskId, this.getMetricValue(model[0].flowrun.apps[i].tasks[j].metrics)]);
        }
      }
    }
    controller.set('flowrun', model[0].flowrun);
    controller.set('metric_id', model[0].metricId);
    controller.set('arr', metricsArr);
  },

  unloadAll: function() {
    this.store.unloadAll('yarn-flowrun');
    this.store.unloadAll('yarn-app-flowrun');
    this.store.unloadAll('yarn-entity');
  },

  actions: {
    error(err/*, transition*/) {
      var errObj = ErrorUtils.stripErrorCodeAndMessageFromError(err);
      this.transitionTo('timeline-error', errObj);
    }
  }
});
