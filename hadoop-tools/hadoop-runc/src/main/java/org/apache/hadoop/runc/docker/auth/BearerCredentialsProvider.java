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

package org.apache.hadoop.runc.docker.auth;

import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.util.Args;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Contract(threading = ThreadingBehavior.SAFE)
public class BearerCredentialsProvider implements CredentialsProvider {

  private final ConcurrentHashMap<AuthScope, Credentials> credMap;

  public BearerCredentialsProvider() {
    super();
    this.credMap = new ConcurrentHashMap<AuthScope, Credentials>();
  }

  /**
   * Find matching {@link Credentials credentials} for the given
   * authentication scope.
   *
   * @param map       the credentials hash map
   * @param authscope the {@link AuthScope authentication scope}
   * @return the credentials
   */
  private static Credentials matchCredentials(
      final Map<AuthScope, Credentials> map,
      final AuthScope authscope) {
    // see if we get a direct hit
    Credentials creds = map.get(authscope);
    if (creds == null) {
      // Nope.
      // Do a full scan
      int bestMatchFactor = -1;
      AuthScope bestMatch = null;
      for (final AuthScope current : map.keySet()) {
        final int factor = authscope.match(current);
        if (factor > bestMatchFactor) {
          bestMatchFactor = factor;
          bestMatch = current;
        }
      }
      if (bestMatch != null) {
        creds = map.get(bestMatch);
      }
    }
    return creds;
  }

  @Override
  public void setCredentials(
      final AuthScope authscope,
      final Credentials credentials) {
    Args.notNull(authscope, "Authentication scope");
    credMap.put(authscope, credentials);
  }

  @Override
  public Credentials getCredentials(final AuthScope authscope) {
    Args.notNull(authscope, "Authentication scope");
    Credentials creds = matchCredentials(this.credMap, authscope);
    if (creds == null) {
      creds = new BearerCredentials(authscope);
      credMap.put(authscope, creds);
    }
    return creds;
  }

  @Override
  public void clear() {
    this.credMap.clear();
  }

  @Override
  public String toString() {
    return credMap.toString();
  }

}
