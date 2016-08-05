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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

/*
 * Collection of activity operation states.
 */
public enum ActivityState {
  // default state when adding a new activity in node allocation
  DEFAULT,
  // container is allocated to sub-queues/applications or this queue/application
  ACCEPTED,
  // queue or application voluntarily give up to use the resource OR
  // nothing allocated
  SKIPPED,
  // container could not be allocated to sub-queues or this application
  REJECTED,
  ALLOCATED, // successfully allocate a new non-reserved container
  RESERVED,  // successfully reserve a new container
  RE_RESERVED  // successfully reserve a new container
}
