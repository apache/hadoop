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
 * Enum to define Audit Action types for OzoneManager.
 */
public enum OMAction implements AuditAction {

  // WRITE Actions
  ALLOCATE_BLOCK("ALLOCATE_BLOCK"),
  ALLOCATE_KEY("ALLOCATE_KEY"),
  COMMIT_KEY("COMMIT_KEY"),
  CREATE_VOLUME("CREATE_VOLUME"),
  CREATE_BUCKET("CREATE_BUCKET"),
  CREATE_KEY("CREATE_KEY"),
  DELETE_VOLUME("DELETE_VOLUME"),
  DELETE_BUCKET("DELETE_BUCKET"),
  DELETE_KEY("DELETE_KEY"),
  RENAME_KEY("RENAME_KEY"),
  SET_OWNER("SET_OWNER"),
  SET_QUOTA("SET_QUOTA"),
  UPDATE_VOLUME("UPDATE_VOLUME"),
  UPDATE_BUCKET("UPDATE_BUCKET"),
  UPDATE_KEY("UPDATE_KEY"),
  // READ Actions
  CHECK_VOLUME_ACCESS("CHECK_VOLUME_ACCESS"),
  LIST_BUCKETS("LIST_BUCKETS"),
  LIST_VOLUMES("LIST_VOLUMES"),
  LIST_KEYS("LIST_KEYS"),
  READ_VOLUME("READ_VOLUME"),
  READ_BUCKET("READ_BUCKET"),
  READ_KEY("READ_BUCKET");

  private String action;

  OMAction(String action) {
    this.action = action;
  }

  @Override
  public String getAction() {
    return this.action;
  }

}
