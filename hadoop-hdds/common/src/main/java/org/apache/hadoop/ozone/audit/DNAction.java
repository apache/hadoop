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
 * Enum to define Audit Action types for Datanode.
 */
public enum DNAction implements AuditAction {

  CREATE_CONTAINER,
  READ_CONTAINER,
  UPDATE_CONTAINER,
  DELETE_CONTAINER,
  LIST_CONTAINER,
  PUT_BLOCK,
  GET_BLOCK,
  DELETE_BLOCK,
  LIST_BLOCK,
  READ_CHUNK,
  DELETE_CHUNK,
  WRITE_CHUNK,
  LIST_CHUNK,
  COMPACT_CHUNK,
  PUT_SMALL_FILE,
  GET_SMALL_FILE,
  CLOSE_CONTAINER,
  GET_COMMITTED_BLOCK_LENGTH;

  @Override
  public String getAction() {
    return this.toString();
  }

}
