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
import AppTableController from './app-table-columns';
import TableDefinition from 'em-table/utils/table-definition';

export default AppTableController.extend({
  queryParams: ['searchText', 'sortColumnId', 'sortOrder', 'pageNum', 'rowCount'],
  tableDefinition: TableDefinition.create({
    searchType: 'manual',
    sortColumnId: 'stTime',
    sortOrder: 'desc',
    rowCount: 25,
    minValuesToDisplay: 1,
    enableFaceting: true
  }),
  searchText: Ember.computed.alias('tableDefinition.searchText'),
  sortColumnId: Ember.computed.alias('tableDefinition.sortColumnId'),
  sortOrder: Ember.computed.alias('tableDefinition.sortOrder'),
  pageNum: Ember.computed.alias('tableDefinition.pageNum'),
  rowCount: Ember.computed.alias('tableDefinition.rowCount'),

  breadcrumbs: [{
    text: "Home",
    routeName: 'application'
  }, {
    text: "Services",
    routeName: 'yarn-services',
  }],

  getFinishedServicesDataForDonutChart: Ember.computed('model.apps', function() {

    var finishdApps = 0;
    var failedApps = 0;
    var killedApps = 0;

    this.get('model.apps').forEach(function(service){
      if (service.get('state') === "FINISHED") {
        finishdApps++;
      }

      if (service.get('state') === "FAILED") {
        failedApps++;
      }

     if (service.get('state') === "KILLED") {
        killedApps++;
      }
    });

    var arr = [];
    arr.push({
      label: "Completed",
      value: finishdApps
    });
    arr.push({
      label: "Killed",
      value: killedApps
    });
    arr.push({
      label: "Failed",
      value: failedApps
    });

    return arr;
  }),


  getRunningServicesDataForDonutChart: Ember.computed('model.apps', function() {
    var pendingApps = 0;
    var runningApps = 0;

    this.get('model.apps').forEach(function(service){
    if (service.get('state') === "RUNNING") {
        runningApps++;
      }

     if (service.get('state') === "ACCEPTED" ||
          service.get('state') === "SUBMITTED" ||
          service.get('state') === "NEW" ||
          service.get('state') === "NEW_SAVING") {
        pendingApps++;
      }
    });

    var arr = [];
    arr.push({
      label: "Pending",
      value: pendingApps
    });
    arr.push({
      label: "Running",
      value: runningApps
    });

    return arr;
  }),

});
