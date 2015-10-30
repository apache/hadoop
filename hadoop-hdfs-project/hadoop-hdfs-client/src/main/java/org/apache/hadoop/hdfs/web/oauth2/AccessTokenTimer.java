/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web.oauth2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Timer;

/**
 * Access tokens generally expire.  This timer helps keep track of that.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AccessTokenTimer {
  public static final long EXPIRE_BUFFER_MS = 30 * 1000L;

  private final Timer timer;

  /**
   * When the current access token will expire in milliseconds since
   * epoch.
   */
  private long nextRefreshMSSinceEpoch;

  public AccessTokenTimer() {
    this(new Timer());
  }

  /**
   *
   * @param timer Timer instance for unit testing
   */
  public AccessTokenTimer(Timer timer) {
    this.timer = timer;
    this.nextRefreshMSSinceEpoch = 0;
  }

  /**
   * Set when the access token will expire as reported by the oauth server,
   * ie in seconds from now.
   * @param expiresIn Access time expiration as reported by OAuth server
   */
  public void setExpiresIn(String expiresIn) {
    this.nextRefreshMSSinceEpoch = convertExpiresIn(timer, expiresIn);
  }

  /**
   * Set when the access token will expire in milliseconds from epoch,
   * as required by the WebHDFS configuration.  This is a bit hacky and lame.
   *
   * @param expiresInMSSinceEpoch Access time expiration in ms since epoch.
   */
  public void setExpiresInMSSinceEpoch(String expiresInMSSinceEpoch){
    this.nextRefreshMSSinceEpoch = Long.parseLong(expiresInMSSinceEpoch);
  }

  /**
   * Get next time we should refresh the token.
   *
   * @return Next time since epoch we'll need to refresh the token.
   */
  public long getNextRefreshMSSinceEpoch() {
    return nextRefreshMSSinceEpoch;
  }

  /**
   * Return true if the current token has expired or will expire within the
   * EXPIRE_BUFFER_MS (to give ample wiggle room for the call to be made to
   * the server).
   */
  public boolean shouldRefresh() {
    long lowerLimit = nextRefreshMSSinceEpoch - EXPIRE_BUFFER_MS;
    long currTime = timer.now();
    return currTime > lowerLimit;
  }

  /**
   * The expires_in param from OAuth is in seconds-from-now.  Convert to
   * milliseconds-from-epoch
   */
  static Long convertExpiresIn(Timer timer, String expiresInSecs) {
    long expiresSecs = Long.parseLong(expiresInSecs);
    long expiresMs = expiresSecs * 1000;
    return timer.now() + expiresMs;
  }

}
