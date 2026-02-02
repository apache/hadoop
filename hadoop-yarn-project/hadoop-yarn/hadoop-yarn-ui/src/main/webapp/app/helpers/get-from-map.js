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


function getNestedValue(obj, path) {
  return path.split('.').reduce((acc, key) => {
    if (acc === null || acc === undefined) {
      return undefined;
    }

   /* Handle array indexing.
   Sample input data:
    {
      "partitionKey": {
        "configuredMinResource": {
          "resourceInformations": {
            "resourceInformation": [
              {
                "maximumAllocation": 1024
              },
              {
                "maximumAllocation": 88
              }
            ]
          }
        }
      }
    }
  */
    const arrayMatch = key.match(/(\w+)\[(\d+)\]/);
    if (arrayMatch) {
      const arrayKey = arrayMatch[1];
      const arrayIndex = parseInt(arrayMatch[2], 10);
      return acc[arrayKey] && acc[arrayKey][arrayIndex];
    }

    return acc[key];
  }, obj);
}

export function getFromMap(params, hash) {
  /*
  Extract map values based on the key provided and the path to the nested value
  Example:
  XPATH from the metrics: /scheduler/schedulerInfo/capacities/queueCapacitiesByPartition[3]/configuredMinResource/resourceInformations/resourceInformation[2]/maximumAllocation
  The partition map is: queueCapacitiesByPartition and accessed as "partitionMap" from the code defined in the models/yarn-queue/capacity-queue.js
  The supplied hash.map is partitionMap
  The supplied key is the partition name (nodelabel), e.g. "customPartition"
  The parameter is "configuredMinResource/resourceInformations/resourceInformation[2]/maximumAllocation"
  The returned value is the value of the maximumAllocation, which in this case will be the number of vCores present.
  */
  return getNestedValue(hash.map[hash.key], hash.parameter);
}

export default Ember.Helper.helper(getFromMap);
