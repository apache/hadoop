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
import ColumnDef from 'em-table/utils/column-definition';
import TableDefinition from 'em-table/utils/table-definition';

function _createColumns() {
  var columns = [];

  columns.push({
    id: 'name',
    headerTitle: 'Name',
    contentPath: 'name',
    observePath: true,
    cellComponentName: 'em-table-html-cell',
    getCellContent: function(row) {
      var plainName = row.name;
      if (plainName.indexOf('MAP:') > -1 || plainName.indexOf('REDUCE:') > -1) {
        plainName = plainName.substring(plainName.indexOf(':') + 1);
      }
      return `<span>${plainName}</span>`;
    }
  }, {
    id: 'value',
    headerTitle: 'Value',
    contentPath: 'value',
    observePath: true
  });

  return ColumnDef.make(columns);
}

export default Ember.Controller.extend({
  mapMetrics: null,
  reduceMetrics: null,
  generalMetrics: null,

  tableDefinition: TableDefinition.create({
    searchType: 'manual',
  }),

  columns: Ember.computed(function() {
    return _createColumns(this.get('model.flowrun_uid'));
  }),

  metricsObserver: Ember.observer('model.flowrun', function() {
    var metrics = this.get('model.flowrun.metrics');
    var mapConfigs = [],
        reduceConfigs = [],
        generalConfigs = [];

    metrics.forEach(function(metric) {
      let id = metric.id;
      if (id.startsWith('MAP:')) {
        mapConfigs.push(metric);
      } else if (id.startsWith('REDUCE:')) {
        reduceConfigs.push(metric);
      } else {
        generalConfigs.push(metric);
      }
    }, this);

    this.set('mapMetrics', mapConfigs);
    this.set('reduceMetrics', reduceConfigs);
    this.set('generalMetrics', generalConfigs);
  }),

  mapConfigRows: Ember.computed('mapMetrics', function() {
    var row = null,
        data = [];

    this.get('mapMetrics').forEach(function(map) {
      let value = map.values[Object.keys(map.values)[0]];
      row = Ember.Object.create({
        name: map.id,
        value: value
      });
      data.push(row);
    }, this);

    return Ember.A(data);
  }),

  reduceConfigRows: Ember.computed('reduceMetrics', function() {
    var row = null,
        data = [];

    this.get('reduceMetrics').forEach(function(map) {
      let value = map.values[Object.keys(map.values)[0]];
      row = Ember.Object.create({
        name: map.id,
        value: value
      });
      data.push(row);
    }, this);

    return Ember.A(data);
  }),

  generalConfigRows: Ember.computed('generalMetrics', function() {
    var row = null,
        data = [];

    this.get('generalMetrics').forEach(function(map) {
      let value = map.values[Object.keys(map.values)[0]];
      row = Ember.Object.create({
        name: map.id,
        value: value
      });
      data.push(row);
    }, this);

    return Ember.A(data);
  })
});
