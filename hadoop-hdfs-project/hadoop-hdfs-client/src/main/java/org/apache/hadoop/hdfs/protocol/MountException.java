/**
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

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Exception thrown by the MountManager.
 */
public class MountException extends IOException {
  public static final String MOUNT_ALREADY_EXISTS =
      "Mount already exists: ";
  public static final String MOUNT_DOES_NOT_EXIST =
      "Mount is not found: ";

  public static MountException mountAlreadyExistsException(Path mountPath) {
    return new MountException(MOUNT_ALREADY_EXISTS + mountPath.toString());
  }

  public static MountException mountDoesNotExistException(String mountId) {
    return new MountException(MOUNT_DOES_NOT_EXIST + mountId);
  }

  public MountException(String message) {
    super(message);
  }

  public MountException(String message, Throwable cause) {
    super(message, cause);
  }
}
