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


import { MUTATION_OPERATIONS } from './constants';

export type ConfigProperty = {
  name: string;
  value: string;
};

export type ConfigData = {
  property: ConfigProperty[];
};

export type ConfigEntry = {
  key: string;
  value: string;
};

export type QueueMutationParams = {
  'queue-name': string;
  params: {
    entry: ConfigEntry[];
  };
};

export type GlobalUpdateParams = {
  entry: ConfigEntry[];
};

export type SchedConfUpdateInfo = {
  [MUTATION_OPERATIONS.ADD_QUEUE]?: QueueMutationParams[];
  [MUTATION_OPERATIONS.UPDATE_QUEUE]?: QueueMutationParams[];
  [MUTATION_OPERATIONS.REMOVE_QUEUE]?: string | string[];
  [MUTATION_OPERATIONS.GLOBAL_UPDATES]?: GlobalUpdateParams[];
};
