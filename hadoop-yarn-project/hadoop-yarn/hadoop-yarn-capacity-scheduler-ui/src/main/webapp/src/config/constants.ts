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
 * Configuration constants for YARN Scheduler UI
 */

/**
 * YARN configuration property that controls read-only mode for the UI.
 * When set to 'true', the UI will allow viewing and staging changes but prevent mutations.
 */
export const READ_ONLY_PROPERTY = 'yarn.webapp.scheduler-ui.read-only.enable';

/**
 * YARN configuration property that controls HTTP authentication for web UIs and REST APIs.
 */
export const HTTP_AUTH_PROPERTY = 'hadoop.http.authentication.type';
