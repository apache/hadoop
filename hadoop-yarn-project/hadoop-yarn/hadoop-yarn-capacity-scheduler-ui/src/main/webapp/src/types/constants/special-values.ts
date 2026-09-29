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


export const SPECIAL_VALUES = {
  ROOT_QUEUE_NAME: 'root',
  GLOBAL_QUEUE_PATH: 'global',
  ALL_USERS_ACL: '*',
  NO_USERS_ACL: ' ',
  DEFAULT_PARTITION: '',
  QUEUE_MARKER: '__queue__',
  MAPPING_RULE_JSON_PROPERTY: 'yarn.scheduler.capacity.mapping-rule-json',
  MAPPING_RULE_FORMAT_PROPERTY: 'yarn.scheduler.capacity.mapping-rule-format',
  LEGACY_MODE_PROPERTY: 'yarn.scheduler.capacity.legacy-queue-mode.enabled',
} as const;
