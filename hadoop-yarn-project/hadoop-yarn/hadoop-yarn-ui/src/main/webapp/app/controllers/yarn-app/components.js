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

export default Ember.Controller.extend({
  queryParams: ["service"],
  service: undefined,

  tableColumns: Ember.computed('model.appId', 'model.serviceName', function() {
    var cols = [];
    var service = this.get('model.serviceName');
    var appId = this.get('model.appId');

    cols.push({
      id: 'name',
      headerTitle: 'Component',
      contentPath: 'name',
      cellComponentName: 'em-table-linked-cell',
      getCellContent: function(row) {
        return {
          displayText: row.get('name'),
          href: `#/yarn-component-instances/${row.get('name')}/info?service=${service}&&appid=${appId}`
        };
      }
    }, {
      id: 'vcores',
      headerTitle: 'VCores',
      contentPath: 'vcores'
    }, {
      id: 'memory',
      headerTitle: 'Memory (MB)',
      contentPath: 'memory'
    }, {
      id: 'instances',
      headerTitle: 'Number of Instances',
      contentPath: 'instances',
      observePath: true
    });

    return ColumnDef.make(cols);
  })
});
