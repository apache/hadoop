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

export default Ember.Controller.extend({
  queryParams: ["service", "appid"],
  appid: undefined,
  service: undefined,

  tableDefinition: TableDefinition.create({
    searchType: 'manual',
  }),

  tableColumns: Ember.computed('model.appId', 'model.serviceName', function() {
    var cols = [];
    var appId = this.get('model.appId');
    var serviceName = this.get('model.serviceName');

    cols.push({
      id: 'instanceName',
      headerTitle: 'Component Instance',
      contentPath: 'instanceName',
      cellComponentName: 'em-table-linked-cell',
      getCellContent: function(row) {
        var component = row.get('component');
        var instance = row.get('instanceName');
        var containerId = row.get('containerId');
        return {
          text: instance,
          href: `#/yarn-component-instance/${component}/instances/${instance}/info?appid=${appId}&service=${serviceName}&containerid=${containerId}`
        };
      }
    }, {
      id: 'containerId',
      headerTitle: 'Current Container Id',
      contentPath: 'containerId',
      minWidth: '350px'
    }, {
      id: 'state',
      headerTitle: 'State',
      contentPath: 'state'
    }, {
      id: 'startedDate',
      headerTitle: 'Started Time',
      contentPath: 'startedDate'
    }, {
      id: 'logsLink',
      headerTitle: 'Logs',
      contentPath: 'logsLink',
      cellComponentName: 'em-table-html-cell',
      getCellContent: function(row) {
        var containerLogUrl = row.get('containerLogURL');
        if (containerLogUrl) {
          return `<a href="${containerLogUrl}&service=${serviceName}">Link</a>`;
        } else {
          return 'N/A';
        }
      }
    });

    return ColumnDef.make(cols);
  })
});
