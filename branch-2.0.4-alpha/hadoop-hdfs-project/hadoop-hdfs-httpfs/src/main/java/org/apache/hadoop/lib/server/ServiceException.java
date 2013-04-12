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

package org.apache.hadoop.lib.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.lang.XException;

/**
 * Exception thrown by {@link Service} implementations.
 */
@InterfaceAudience.Private
public class ServiceException extends ServerException {

  /**
   * Creates an service exception using the specified error code.
   * The exception message is resolved using the error code template
   * and the passed parameters.
   *
   * @param error error code for the XException.
   * @param params parameters to use when creating the error message
   * with the error code template.
   */
  public ServiceException(XException.ERROR error, Object... params) {
    super(error, params);
  }

}
