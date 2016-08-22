/*
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

package org.apache.slider.api;

/**
 * Standard options for roles
 */
public interface RoleKeys {


  /**
   * The name of a role: {@value}
   */
  String ROLE_NAME = "role.name";

  /**
   * The group of a role: {@value}
   */
  String ROLE_GROUP = "role.group";

  /**
   * The prefix of a role: {@value}
   */
  String ROLE_PREFIX = "role.prefix";

  /**
   * Status report: number actually granted : {@value} 
   */
  String ROLE_ACTUAL_INSTANCES = "role.actual.instances";

  /**
   * Status report: number currently requested: {@value} 
   */
  String ROLE_REQUESTED_INSTANCES = "role.requested.instances";

  /**
   * Status report: number currently being released: {@value} 
   */
  String ROLE_RELEASING_INSTANCES = "role.releasing.instances";

  /**
   * Status report: total number that have failed: {@value}
   */
  String ROLE_FAILED_INSTANCES = "role.failed.instances";

  /**
   * Status report: number that have failed recently: {@value}
   */
  String ROLE_FAILED_RECENTLY_INSTANCES = "role.failed.recently.instances";

  /**
   * Status report: number that have failed for node-related issues: {@value}
   */
  String ROLE_NODE_FAILED_INSTANCES = "role.failed.node.instances";

  /**
   * Status report: number that been pre-empted: {@value}
   */
  String ROLE_PREEMPTED_INSTANCES = "role.failed.preempted.instances";

  /**
   * Number of pending anti-affine instances: {@value}
   */
  String ROLE_PENDING_AA_INSTANCES = "role.pending.aa.instances";

  /**
   * Status report: number currently being released: {@value} 
   */
  String ROLE_FAILED_STARTING_INSTANCES = "role.failed.starting.instances";

  /**
   * Extra arguments (non-JVM) to use when starting this role
   */
  String ROLE_ADDITIONAL_ARGS = "role.additional.args";

  /**
   *  JVM heap size for Java applications in MB.  Only relevant for Java applications.
   *  This MUST be less than or equal to the {@link ResourceKeys#YARN_MEMORY} option
   *  {@value}
   */
  String JVM_HEAP = "jvm.heapsize";
  
  /*
   * GC options for Java applications.
   */
  String GC_OPTS = "gc.opts";

  /**
   * JVM options other than heap size. Only relevant for Java applications.
   *  {@value}
   */
  String JVM_OPTS = "jvm.opts";


  /**
   * All keys w/ env. are converted into env variables and passed down
   */
  String ENV_PREFIX = "env.";

  /**
   * Container service record attribute prefix.
   */
  String SERVICE_RECORD_ATTRIBUTE_PREFIX = "service.record.attribute";

}
