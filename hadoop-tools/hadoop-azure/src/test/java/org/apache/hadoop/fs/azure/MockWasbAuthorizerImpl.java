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
        cache.init(conf);
        authRules = new HashMap<>();
    }

    public void addAuthRuleForOwner(String wasbAbsolutePath,
                                    String accessType, boolean access) {
        addAuthRule(wasbAbsolutePath, accessType, "owner", access);
    }

    public void addAuthRule(String wasbAbsolutePath,
                            String accessType, String user, boolean access) {
        wasbAbsolutePath = qualifiedPrefixUrl + wasbAbsolutePath;
        AuthorizationComponent component = wasbAbsolutePath.endsWith("*")
                ? new AuthorizationComponent("^" + wasbAbsolutePath.replace("*", ".*"),
                accessType, user)
                : new AuthorizationComponent(wasbAbsolutePath, accessType, user);

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
        try {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            currentUserShortName = ugi.getShortUserName();
        } catch (Exception e) {
            //no op
        }

        // In case of root("/"), owner match does not happen
        // because owner is returned as empty string.
        // we try to force owner match just for purpose of tests
        // to make sure all operations work seemlessly with owner.
        if (StringUtils.equalsIgnoreCase(wasbAbsolutePath, qualifiedPrefixUrl + "/")) {
            owner = currentUserShortName;
        }

        AuthorizationComponent component = new AuthorizationComponent(wasbAbsolutePath,
                accessType, currentUserShortName);

        return processRules(authRules, component, owner);
    }

    private boolean processRules(Map<AuthorizationComponent, Boolean> authRules,
                                 AuthorizationComponent component, String owner) {

        // Direct match of rules and access request
        if (authRules.containsKey(component)) {
            return authRules.get(component);
        } else {
            // Regex-pattern match if we don't have a straight match for path
            // Current user match if we don't have a owner match
            for (Map.Entry<AuthorizationComponent, Boolean> entry : authRules.entrySet()) {
                AuthorizationComponent key = entry.getKey();
                String keyPath = key.getWasbAbsolutePath();
                String keyAccess = key.getAccessType();
                String keyUser = key.getUser();

                boolean foundMatchingOwnerRule = keyPath.equals(component.getWasbAbsolutePath())
                        && keyAccess.equals(component.getAccessType())
                        && keyUser.equalsIgnoreCase("owner")
                        && owner.equals(component.getUser());

                boolean foundMatchingPatternRule = keyPath.endsWith("*")
                        && Pattern.matches(keyPath, component.getWasbAbsolutePath())
                        && keyAccess.equals(component.getAccessType())
                        && keyUser.equalsIgnoreCase(component.getUser());

                boolean foundMatchingPatternOwnerRule = keyPath.endsWith("*")
                        && Pattern.matches(keyPath, component.getWasbAbsolutePath())
                        && keyAccess.equals(component.getAccessType())
                        && keyUser.equalsIgnoreCase("owner")
                        && owner.equals(component.getUser());

                if (foundMatchingOwnerRule
                        || foundMatchingPatternRule
                        || foundMatchingPatternOwnerRule) {
                    return entry.getValue();
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
      private final String user;

      AuthorizationComponent(String wasbAbsolutePath,
          String accessType, String user) {
        this.wasbAbsolutePath = wasbAbsolutePath;
        this.accessType = accessType;
        this.user = user;
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
          getAccessType().equals(this.accessType)
          && ((AuthorizationComponent) obj).
          getUser().equals(this.user);
      }

      public String getWasbAbsolutePath() {
        return this.wasbAbsolutePath;
      }

      public String getAccessType() {
        return accessType;
      }

      public String getUser() {
        return user;
      }
   }
}
