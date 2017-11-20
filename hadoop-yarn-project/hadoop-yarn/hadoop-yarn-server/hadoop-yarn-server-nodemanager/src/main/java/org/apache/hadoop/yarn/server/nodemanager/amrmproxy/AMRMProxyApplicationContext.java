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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.Context;

/**
 * Interface that can be used by the intercepter plugins to get the information
 * about one application.
 *
 */
public interface AMRMProxyApplicationContext {

  /**
   * Gets the configuration object instance.
   * @return the configuration object.
   */
  Configuration getConf();

  /**
   * Gets the application attempt identifier.
   * @return the application attempt identifier.
   */
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * Gets the application submitter.
   * @return the application submitter
   */
  String getUser();

  /**
   * Gets the application's AMRMToken that is issued by the RM.
   * @return the application's AMRMToken that is issued by the RM.
   */
  Token<AMRMTokenIdentifier> getAMRMToken();

  /**
   * Gets the application's local AMRMToken issued by the proxy service.
   * @return the application's local AMRMToken issued by the proxy service.
   */
  Token<AMRMTokenIdentifier> getLocalAMRMToken();

  /**
   * Gets the NMContext object.
   * @return the NMContext.
   */
  Context getNMCotext();

  /**
   * Gets the credentials of this application.
   *
   * @return the credentials.
   */
  Credentials getCredentials();

  /**
   * Gets the registry client.
   *
   * @return the registry.
   */
  RegistryOperations getRegistryClient();

}