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

import TableDef from 'em-table/utils/table-definition';
import ColumnDef from 'em-table/utils/column-definition';

import YarnConf from '../../models/yarn-conf';

export default Ember.Controller.extend({
  coreTableDefinition: TableDef.create({
    searchType: 'manual',
  }),

  mapredTableDefinition: TableDef.create({
    searchType: 'manual',
  }),

  yarnTableDefinition: TableDef.create({
    searchType: 'manual',
  }),

  init: function () {
    var that = this;
    this.get('store').query('yarn-conf', {})
      .then(function(conf) {
        let coreProps = conf.filter(function(o) {
          return o.get('source') == 'core-default.xml';
        });
        that.set('rowsForCoreColumnsFromModel', coreProps);
        let mapredProps = conf.filter(function(o) {
          return o.get('source') == 'mapred-default.xml';
        });
        that.set('rowsForMapredColumnsFromModel', mapredProps);
        let yarnProps = conf.filter(function(o) {
          return o.get('source') == 'yarn-default.xml';
        });
        that.set('rowsForYarnColumnsFromModel', yarnProps);
      });
  },

  columnsFromModel: ColumnDef.makeFromModel(YarnConf),

});
