/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit;

/**
 * Enum to define Audit Action types for SCM.
 */
public enum SCMAction implements AuditAction {

  GET_VERSION,
  REGISTER,
  SEND_HEARTBEAT,
  GET_SCM_INFO,
  ALLOCATE_BLOCK,
  DELETE_KEY_BLOCK,
  ALLOCATE_CONTAINER,
  GET_CONTAINER,
  GET_CONTAINER_WITH_PIPELINE,
  LIST_CONTAINER,
  LIST_PIPELINE,
  CLOSE_PIPELINE,
  DELETE_CONTAINER,
  IN_SAFE_MODE,
  FORCE_EXIT_SAFE_MODE;

  @Override
  public String getAction() {
    return this.toString();
  }

}
