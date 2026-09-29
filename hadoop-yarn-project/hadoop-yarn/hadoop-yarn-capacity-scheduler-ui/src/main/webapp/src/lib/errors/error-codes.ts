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


/**
 * Error codes for better error handling
 */
export const ERROR_CODES = {
  LOAD_INITIAL_DATA_FAILED: 'LOAD_INITIAL_DATA_FAILED',
  REFRESH_SCHEDULER_FAILED: 'REFRESH_SCHEDULER_FAILED',
  APPLY_CHANGES_FAILED: 'APPLY_CHANGES_FAILED',
  INVALID_QUEUE_PATH: 'INVALID_QUEUE_PATH',
  INVALID_PROPERTY_NAME: 'INVALID_PROPERTY_NAME',
  INVALID_PROPERTY_VALUE: 'INVALID_PROPERTY_VALUE',
  INVALID_QUEUE_NAME: 'INVALID_QUEUE_NAME',
  EMPTY_STAGED_CHANGES: 'EMPTY_STAGED_CHANGES',
  API_ERROR: 'API_ERROR',
  NETWORK_ERROR: 'NETWORK_ERROR',
  VALIDATION_ERROR: 'VALIDATION_ERROR',
  ADD_NODE_LABEL_FAILED: 'ADD_NODE_LABEL_FAILED',
  REMOVE_NODE_LABEL_FAILED: 'REMOVE_NODE_LABEL_FAILED',
  ASSIGN_NODE_TO_LABEL_FAILED: 'ASSIGN_NODE_TO_LABEL_FAILED',
  QUEUE_ALREADY_EXISTS: 'QUEUE_ALREADY_EXISTS',
  MUTATION_BLOCKED: 'MUTATION_BLOCKED',
} as const;
