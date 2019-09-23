/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Ozone GRPC token header verifier.
 */
public interface TokenVerifier {
  /**
   * Given a user and tokenStr header, return a UGI object with token if
   * verified.
   * @param user user of the request
   * @param tokenStr token str of the request
   * @return UGI
   * @throws SCMSecurityException
   */
  UserGroupInformation verify(String user, String tokenStr)
      throws SCMSecurityException;
}
