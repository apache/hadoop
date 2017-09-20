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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.Path;

/**
 * A mock wasb authorizer implementation.
 */

public class MockWasbAuthorizerImpl implements WasbAuthorizerInterface {

  private Map<AuthorizationComponent, Boolean> authRules;
  private boolean performOwnerMatch;
  private CachingAuthorizer<CachedAuthorizerEntry, Boolean> cache;

  // The full qualified URL to the root directory
  private String qualifiedPrefixUrl;

  public MockWasbAuthorizerImpl(NativeAzureFileSystem fs) {
    qualifiedPrefixUrl = new Path("/").makeQualified(fs.getUri(),
        fs.getWorkingDirectory())
        .toString().replaceAll("/$", "");
    cache = new CachingAuthorizer<>(TimeUnit.MINUTES.convert(5L, TimeUnit.MINUTES), "AUTHORIZATION");
  }

  @Override
  public void init(Configuration conf) {
    init(conf, false);
  }

  /*
  authorization matches owner with currentUserShortName while evaluating auth rules
  if currentUserShortName is set to a string that is not empty
  */
  public void init(Configuration conf, boolean matchOwner) {
    cache.init(conf);
    authRules = new HashMap<>();
    this.performOwnerMatch = matchOwner;
  }

  public void addAuthRule(String wasbAbsolutePath,
      String accessType, boolean access) {
    wasbAbsolutePath = qualifiedPrefixUrl + wasbAbsolutePath;
    AuthorizationComponent component = wasbAbsolutePath.endsWith("*")
        ? new AuthorizationComponent("^" + wasbAbsolutePath.replace("*", ".*"),
        accessType)
        : new AuthorizationComponent(wasbAbsolutePath, accessType);

    this.authRules.put(component, access);
  }

  @Override
  public boolean authorize(String wasbAbsolutePath,
      String accessType,
      String owner)
      throws WasbAuthorizationException {

    if (wasbAbsolutePath.endsWith(
        NativeAzureFileSystem.FolderRenamePending.SUFFIX)) {
      return true;
    }

    CachedAuthorizerEntry cacheKey = new CachedAuthorizerEntry(wasbAbsolutePath, accessType, owner);
    Boolean cacheresult = cache.get(cacheKey);
    if (cacheresult != null) {
      return cacheresult;
    }

    boolean authorizeresult = authorizeInternal(wasbAbsolutePath, accessType, owner);
    cache.put(cacheKey, authorizeresult);

    return authorizeresult;
  }

  private boolean authorizeInternal(String wasbAbsolutePath, String accessType, String owner)
      throws WasbAuthorizationException {

    String currentUserShortName = "";
    if (this.performOwnerMatch) {
      try {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        currentUserShortName = ugi.getShortUserName();
      } catch (Exception e) {
        //no op
      }
    }

    // In case of root("/"), owner match does not happen because owner is returned as empty string.
    // we try to force owner match just for purpose of tests to make sure all operations work seemlessly with owner.
    if (this.performOwnerMatch
        && StringUtils.equalsIgnoreCase(wasbAbsolutePath,
        qualifiedPrefixUrl + "/")) {
      owner = currentUserShortName;
    }

    boolean shouldEvaluateOwnerAccess = owner != null && !owner.isEmpty()
        && this.performOwnerMatch;

    boolean isOwnerMatch = StringUtils.equalsIgnoreCase(currentUserShortName,
        owner);

    AuthorizationComponent component =
        new AuthorizationComponent(wasbAbsolutePath, accessType);

    if (authRules.containsKey(component)) {
      return shouldEvaluateOwnerAccess ? isOwnerMatch && authRules.get(
          component) : authRules.get(component);
    } else {
      // Regex-pattern match if we don't have a straight match
      for (Map.Entry<AuthorizationComponent, Boolean> entry : authRules.entrySet()) {
        AuthorizationComponent key = entry.getKey();
        String keyPath = key.getWasbAbsolutePath();
        String keyAccess = key.getAccessType();

        if (keyPath.endsWith("*") && Pattern.matches(keyPath, wasbAbsolutePath)
            && keyAccess.equals(accessType)) {
          return shouldEvaluateOwnerAccess
              ? isOwnerMatch && entry.getValue()
              : entry.getValue();
        }
      }
      return false;
    }
  }

  public void deleteAllAuthRules() {
    authRules.clear();
    cache.clear();
  }

  private static class AuthorizationComponent {

    private final String wasbAbsolutePath;
    private final String accessType;

    AuthorizationComponent(String wasbAbsolutePath,
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

      return ((AuthorizationComponent) obj).
          getWasbAbsolutePath().equals(this.wasbAbsolutePath)
          && ((AuthorizationComponent) obj).
          getAccessType().equals(this.accessType);
    }

    public String getWasbAbsolutePath() {
      return this.wasbAbsolutePath;
    }

    public String getAccessType() {
      return accessType;
    }
  }
}
