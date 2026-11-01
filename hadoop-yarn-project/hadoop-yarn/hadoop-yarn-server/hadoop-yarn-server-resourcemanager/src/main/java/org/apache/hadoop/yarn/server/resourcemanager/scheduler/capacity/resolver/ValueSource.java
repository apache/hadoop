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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Where a resolved queue configuration value came from, in the order the
 * resolution steps are tried.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum ValueSource {
  /** An explicit key under the queue's own path. */
  QUEUE,
  /** A v2 template ({@code auto-queue-creation-v2.*template}) of the parent. */
  TEMPLATE_V2,
  /** The v1 {@code leaf-queue-template} of a managed parent. */
  TEMPLATE_V1,
  /** The parent queue's resolved value. */
  PARENT,
  /** A scheduler-wide key or input. */
  GLOBAL,
  /** The built-in default. */
  DEFAULT,
  /** A formula over other resolved values. */
  DERIVED
}
