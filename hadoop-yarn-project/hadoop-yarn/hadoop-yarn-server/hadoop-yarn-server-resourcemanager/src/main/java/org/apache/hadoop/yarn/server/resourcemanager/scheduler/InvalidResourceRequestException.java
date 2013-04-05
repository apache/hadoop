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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.YarnException;

/**
 * The exception is thrown when the requested resource is out of the range
 * of the configured lower and upper resource boundaries.
 *
 */
public class InvalidResourceRequestException extends YarnException {

  public InvalidResourceRequestException(Throwable cause) {
    super(cause);
  }

  public InvalidResourceRequestException(String message) {
    super(message);
  }

  public InvalidResourceRequestException(String message, Throwable cause) {
    super(message, cause);
  }

}
