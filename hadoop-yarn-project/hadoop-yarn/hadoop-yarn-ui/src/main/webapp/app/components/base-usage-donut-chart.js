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

import DonutChart from 'yarn-ui/components/donut-chart';

export default DonutChart.extend({
  mergeLongTails: function(usages, nItemsKept) {
    var arr = [];
    for (var i = 0; i < Math.min(usages.length, nItemsKept); i++) {
      arr.push(usages[i]);
    }

    var others = {
      label: "Used by others",
      value: 0
    };

    for (i = nItemsKept; i < usages.length; i++) {
      others.value += Number(usages[i].value);
    }
    others.value = others.value.toFixed(2);

    arr.push(others);

    return arr;
  }
});