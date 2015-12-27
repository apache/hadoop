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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Container property encoding execution semantics.
 *
 * <p>
 * The execution types are the following:
 * <ul>
 *   <li>{@link #GUARANTEED} - this container is guaranteed to start its
 *   execution, once the corresponding start container request is received by
 *   an NM.
 *   <li>{@link #OPPORTUNISTIC} - the execution of this container may not start
 *   immediately at the NM that receives the corresponding start container
 *   request (depending on the NM's available resources). Moreover, it may be
 *   preempted if it blocks a GUARANTEED container from being executed.
 * </ul>
 */
@Public
@Evolving
public enum ExecutionType {
  GUARANTEED, OPPORTUNISTIC
}
