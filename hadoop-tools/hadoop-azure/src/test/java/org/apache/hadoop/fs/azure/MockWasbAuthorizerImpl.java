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

package org.apache.hadoop.fs.azure;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

/**
 * A mock wasb authorizer implementation.
 */

public class MockWasbAuthorizerImpl implements WasbAuthorizerInterface {

  private Map<AuthorizationComponent, Boolean> authRules;

  @Override
  public void init(Configuration conf) {
    authRules = new HashMap<AuthorizationComponent, Boolean>();
  }

  public void addAuthRule(String wasbAbsolutePath,
      String accessType, boolean access) {

    AuthorizationComponent component = wasbAbsolutePath.endsWith("*")
        ? new AuthorizationComponent("^" + wasbAbsolutePath.replace("*", ".*"), accessType)
        : new AuthorizationComponent(wasbAbsolutePath, accessType);

    this.authRules.put(component, access);
  }

  @Override
  public boolean authorize(String wasbAbsolutePath, String accessType)
      throws WasbAuthorizationException {

    if (wasbAbsolutePath.endsWith(NativeAzureFileSystem.FolderRenamePending.SUFFIX)) {
      return true;
    }

    AuthorizationComponent component =
        new AuthorizationComponent(wasbAbsolutePath, accessType);

    if (authRules.containsKey(component)) {
      return authRules.get(component);
    } else {
      // Regex-pattern match if we don't have a straight match
      for (Map.Entry<AuthorizationComponent, Boolean> entry : authRules.entrySet()) {
        AuthorizationComponent key = entry.getKey();
        String keyPath = key.getWasbAbsolutePath();
        String keyAccess = key.getAccessType();

        if (keyPath.endsWith("*") && Pattern.matches(keyPath, wasbAbsolutePath) && keyAccess.equals(accessType)) {
          return entry.getValue();
        }
      }
      return false;
    }
  }
}

class AuthorizationComponent {

  private String wasbAbsolutePath;
  private String accessType;

  public AuthorizationComponent(String wasbAbsolutePath,
      String accessType) {
    this.wasbAbsolutePath = wasbAbsolutePath;
    this.accessType = accessType;
  }

  @Override
  public int hashCode() {
    return this.wasbAbsolutePath.hashCode() ^ this.accessType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {

    if (obj == this) {
      return true;
    }

    if (obj == null
        || !(obj instanceof AuthorizationComponent)) {
      return false;
    }

    return ((AuthorizationComponent)obj).
              getWasbAbsolutePath().equals(this.wasbAbsolutePath)
            && ((AuthorizationComponent)obj).
              getAccessType().equals(this.accessType);
  }

  public String getWasbAbsolutePath() {
    return this.wasbAbsolutePath;
  }

  public String getAccessType() {
    return accessType;
  }
}