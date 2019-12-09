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

module.exports = { // YARN UI App configurations
    hosts: {
      localBaseAddress: "",
      timelineWebAddress: "localhost:8188",
      timelineV1WebAddress: "localhost:8188",
      rmWebAddress: "localhost:8088",
      protocolScheme: "http:",
      isSecurityEnabled: ""
    },
    namespaces: {
      timeline: 'ws/v1/applicationhistory',
      timelineService: 'ws/v2/timeline/apps',
      cluster: 'ws/v1/cluster',
      metrics: 'ws/v1/cluster/metrics',
      timelineV2: 'ws/v2/timeline',
      dashService: 'app/v1/services',
      node: '{nodeAddress}/ws/v1/node'
    },
};
