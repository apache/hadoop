/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

/**
 * Exception thrown by the MountManager.
 */
public class MountException extends IOException {
  public static final String BACKUP_NAME_ALREADY_EXISTS =
      "Backup name already exists: ";
  public static final String BACKUP_NAME_DOES_NOT_EXIST =
      "Backup name not found: ";

  public static MountException nameAlreadyExistsException(String name) {
    return new MountException(BACKUP_NAME_ALREADY_EXISTS + name);
  }

  public static MountException nameDoesNotExistException(String name) {
    return new MountException(BACKUP_NAME_DOES_NOT_EXIST + name);
  }

  public MountException(String message) {
    super(message);
  }

  public MountException(String message, Throwable cause) {
    super(message, cause);
  }
}
