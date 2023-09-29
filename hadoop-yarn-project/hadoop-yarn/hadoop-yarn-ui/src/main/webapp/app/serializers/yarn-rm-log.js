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

import DS from 'ember-data';

export default DS.JSONAPISerializer.extend({
  normalizeResponse(store, primaryModelClass, payload) {
    const pattern = new RegExp('<A HREF="/logs/.+">|<a href="/logs/.+">', 'g');
    let fileNames = payload.match(pattern);

    if (fileNames == null) {
      return {data : []};
    }

    let logfileNames = [];
    for (var i = 0; i < fileNames.length; i++) {
      var fileNameMatch = fileNames[i].match(/<A HREF="(\/logs\/.+)">.+<\/A>|<a href="(\/logs\/.+)">.+<\/a>/);
      var logFileUrl = fileNameMatch[1] || fileNameMatch[2];
      var logFileName = logFileUrl.replace('logs', '').replace(/\//g, '');

      if (fileNameMatch.length != null) {
        logfileNames.push({
          id: i,
          type: primaryModelClass.modelName,
          attributes: {
            logFileUrl: logFileUrl,
            logFileName: logFileName
          }
        });
      }
    }
    return { data : logfileNames };
  },
});
