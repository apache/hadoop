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
import TableDefinition from 'em-table/utils/table-definition';
import AppTableController from '../app-table-columns';

export default AppTableController.extend({
  queryParams: ['searchText', 'sortColumnId', 'sortOrder', 'pageNum', 'rowCount'],
  tableDefinition: TableDefinition.create({
    enableFaceting: true,
    rowCount: 25
  }),
  searchText: Ember.computed.alias('tableDefinition.searchText'),
  sortColumnId: Ember.computed.alias('tableDefinition.sortColumnId'),
  sortOrder: Ember.computed.alias('tableDefinition.sortOrder'),
  pageNum: Ember.computed.alias('tableDefinition.pageNum'),
  rowCount: Ember.computed.alias('tableDefinition.rowCount')
});
