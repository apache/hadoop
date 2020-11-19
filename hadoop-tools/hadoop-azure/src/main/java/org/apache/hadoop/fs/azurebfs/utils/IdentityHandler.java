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
package org.apache.hadoop.fs.azurebfs.utils;

import java.io.IOException;


/**
 * {@code IdentityHandler} defines the set of methods to support various
 * identity lookup services.
 */
public interface IdentityHandler {

  /**
   * Perform lookup from Service Principal's Object ID to Username.
   * @param originalIdentity AAD object ID.
   * @return User name, if no name found returns empty string.
   * */
  String lookupForLocalUserIdentity(String originalIdentity) throws IOException;

  /**
   * Perform lookup from Security Group's Object ID to Security Group name.
   * @param originalIdentity AAD object ID.
   * @return Security group name, if no name found returns empty string.
   * */
  String lookupForLocalGroupIdentity(String originalIdentity) throws IOException;
}
