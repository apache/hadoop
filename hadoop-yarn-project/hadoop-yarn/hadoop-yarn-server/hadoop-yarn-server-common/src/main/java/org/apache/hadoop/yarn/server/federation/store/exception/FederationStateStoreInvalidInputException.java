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

package org.apache.hadoop.yarn.server.federation.store.exception;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Exception thrown by the {@code FederationMembershipStateStoreInputValidator},
 * {@code FederationApplicationHomeSubClusterStoreInputValidator},
 * {@code FederationPolicyStoreInputValidator} if the input is invalid.
 *
 */
public class FederationStateStoreInvalidInputException extends YarnException {

  /**
   * IDE auto-generated.
   */
  private static final long serialVersionUID = -7352144682711430801L;

  public FederationStateStoreInvalidInputException(Throwable cause) {
    super(cause);
  }

  public FederationStateStoreInvalidInputException(String message) {
    super(message);
  }

  public FederationStateStoreInvalidInputException(String message,
      Throwable cause) {
    super(message, cause);
  }
}
