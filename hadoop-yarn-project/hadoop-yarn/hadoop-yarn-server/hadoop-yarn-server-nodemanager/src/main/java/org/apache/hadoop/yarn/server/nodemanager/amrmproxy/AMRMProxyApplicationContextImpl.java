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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.Context;

/**
 * Encapsulates the information about one application that is needed by the
 * request intercepters.
 *
 */
public class AMRMProxyApplicationContextImpl implements
    AMRMProxyApplicationContext {
  private final Configuration conf;
  private final Context nmContext;
  private final ApplicationAttemptId applicationAttemptId;
  private final String user;
  private Integer localTokenKeyId;
  private Token<AMRMTokenIdentifier> amrmToken;
  private Token<AMRMTokenIdentifier> localToken;

  /**
   * Create an instance of the AMRMProxyApplicationContext.
   * 
   * @param nmContext
   * @param conf
   * @param applicationAttemptId
   * @param user
   * @param amrmToken
   */
  public AMRMProxyApplicationContextImpl(Context nmContext,
      Configuration conf, ApplicationAttemptId applicationAttemptId,
      String user, Token<AMRMTokenIdentifier> amrmToken,
      Token<AMRMTokenIdentifier> localToken) {
    this.nmContext = nmContext;
    this.conf = conf;
    this.applicationAttemptId = applicationAttemptId;
    this.user = user;
    this.amrmToken = amrmToken;
    this.localToken = localToken;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public synchronized Token<AMRMTokenIdentifier> getAMRMToken() {
    return amrmToken;
  }

  /**
   * Sets the application's AMRMToken.
   */
  public synchronized void setAMRMToken(
      Token<AMRMTokenIdentifier> amrmToken) {
    this.amrmToken = amrmToken;
  }

  @Override
  public synchronized Token<AMRMTokenIdentifier> getLocalAMRMToken() {
    return this.localToken;
  }

  /**
   * Sets the application's AMRMToken.
   */
  public synchronized void setLocalAMRMToken(
      Token<AMRMTokenIdentifier> localToken) {
    this.localToken = localToken;
    this.localTokenKeyId = null;
  }

  @Private
  public synchronized int getLocalAMRMTokenKeyId() {
    Integer keyId = this.localTokenKeyId;
    if (keyId == null) {
      try {
        if (this.localToken == null) {
          throw new YarnRuntimeException("Missing AMRM token for "
              + this.applicationAttemptId);
        }
        keyId = this.localToken.decodeIdentifier().getKeyId();
        this.localTokenKeyId = keyId;
      } catch (IOException e) {
        throw new YarnRuntimeException("AMRM token decode error for "
            + this.applicationAttemptId, e);
      }
    }
    return keyId;
  }

  @Override
  public Context getNMCotext() {
    return nmContext;
  }
}