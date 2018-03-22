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
import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPISerializer.extend({
  normalizeResponse(store, primaryModelClass, payload, id, requestType) {
    var x2js = new X2JS();
    let props = x2js.xml_str2json(payload);
    let properties = props.configuration.property;
    var convertedPayload = [];
    for (var i = 0; i < properties.length; i++) {
      var row = {
        id: i,
        type: primaryModelClass.modelName,
        attributes: {
          name: properties[i].name,
          source: properties[i].source,
          value: properties[i].value
        }
      };
      convertedPayload.push(row);
    }
    return { data: convertedPayload };
  },
});
