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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * This exception is thrown by {@link YarnConfigurationStore} if it's loading
 * an incompatible persisted schema version.
 */
public class YarnConfStoreVersionIncompatibleException extends
    YarnException {
  private static final long serialVersionUID = -2829858253579013629L;

  public YarnConfStoreVersionIncompatibleException(Throwable cause) {
    super(cause);
  }

  public YarnConfStoreVersionIncompatibleException(String message) {
    super(message);
  }

  public YarnConfStoreVersionIncompatibleException(
      String message, Throwable cause) {
    super(message, cause);
  }
}
