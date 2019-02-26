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
package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.AT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SUPER_USER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SKIP_SUPER_USER_REPLACEMENT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_FILE_OWNER_DOMAINNAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_OVERRIDE_OWNER_SP;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_OVERRIDE_OWNER_SP_LIST;

/**
 * Perform transformation for Azure Active Directory identities used in owner, group and acls.
 */
public class IdentityTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(IdentityTransformer.class);

  private boolean isSecure;
  private String servicePrincipalId;
  private String serviceWhiteList;
  private String domainName;
  private boolean enableShortName;
  private boolean skipUserIdentityReplacement;
  private boolean skipSuperUserReplacement;
  private boolean domainIsSet;
  private static final String UUID_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

  public IdentityTransformer(Configuration configuration) throws IOException {
    Preconditions.checkNotNull(configuration, "configuration");
    this.isSecure = UserGroupInformation.getCurrentUser().isSecurityEnabled();
    this.servicePrincipalId = configuration.get(FS_AZURE_OVERRIDE_OWNER_SP, "");
    this.serviceWhiteList = configuration.get(FS_AZURE_OVERRIDE_OWNER_SP_LIST, "");
    this.domainName = configuration.get(FS_AZURE_FILE_OWNER_DOMAINNAME, "");
    this.enableShortName = configuration.getBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, false);

    // - "servicePrincipalId" and "serviceWhiteList" are required for
    //    transformation between localUserOrGroup and principalId,$superuser
    // - "enableShortName" is required for transformation between shortName and fullyQualifiedName.
    this.skipUserIdentityReplacement = servicePrincipalId.isEmpty() && serviceWhiteList.isEmpty() && !enableShortName;
    this.skipSuperUserReplacement = configuration.getBoolean(FS_AZURE_SKIP_SUPER_USER_REPLACEMENT, false);

    if (enableShortName){
      // need to check the domain setting only when short name is enabled.
      // if shortName is not enabled, transformer won't transform a shortName to
      // a fully qualified name.
      this.domainIsSet = !domainName.isEmpty();
    }
  }

  /**
   * Perform identity transformation for the Get request results in AzureBlobFileSystemStore:
   * getFileStatus(), listStatus(), getAclStatus().
   * Input originalIdentity can be one of the following:
   * 1. $superuser:
   *     by default it will be transformed to local user/group, this can be disabled by setting
   *     "fs.azure.identity.transformer.skip.superuser.replacement" to true.
   *
   * 2. User principal id:
   *     can be transformed to localIdentity, if this principal id matches the principal id set in
   *     "fs.azure.identity.transformer.service.principal.id" and localIdentity is stated in
   *     "fs.azure.identity.transformer.service.principal.substitution.list"
   *
   * 3. User principal name (UPN):
   *     can be transformed to a short name(localIdentity) if originalIdentity is owner name, and
   *     "fs.azure.identity.transformer.enable.short.name" is enabled.
   *
   * @param originalIdentity the original user or group in the get request results: FileStatus, AclStatus.
   * @param isUserName indicate whether the input originalIdentity is an owner name or owning group name.
   * @param localIdentity the local user or group, should be parsed from UserGroupInformation.
   * @return owner or group after transformation.
   * */
  public String transformIdentityForGetRequest(String originalIdentity, boolean isUserName, String localIdentity) {
    if (originalIdentity == null) {
      originalIdentity = localIdentity;
      // localIdentity might be a full name, so continue the transformation.
    }
    // case 1: it is $superuser and replace $superuser config is enabled
    if (!skipSuperUserReplacement && SUPER_USER.equals(originalIdentity)) {
      return localIdentity;
    }

    if (skipUserIdentityReplacement) {
      return originalIdentity;
    }

    // case 2: original owner is principalId set in config, and localUser
    //         is a daemon service specified in substitution list,
    //         To avoid ownership check failure in job task, replace it
    //         to local daemon user/group
    if (originalIdentity.equals(servicePrincipalId) && isInSubstitutionList(localIdentity)) {
      return localIdentity;
    }

    // case 3: If original owner is a fully qualified name, and
    //         short name is enabled, replace with shortName.
    if (isUserName && shouldUseShortUserName(originalIdentity)) {
      return getShortName(originalIdentity);
    }

    return originalIdentity;
  }

  /**
   * Perform Identity transformation when setting owner on a path.
   * There are four possible input:
   * 1.short name; 2.$superuser; 3.Fully qualified name; 4. principal id.
   *
   * short name could be transformed to:
   *    - A service principal id or $superuser, if short name belongs a daemon service
   *      stated in substitution list AND "fs.azure.identity.transformer.service.principal.id"
   *      is set with $superuser or a principal id.
   *    - Fully qualified name, if "fs.azure.identity.transformer.domain.name" is set in configuration.
   *
   * $superuser, fully qualified name and principalId should not be transformed.
   *
   * @param userOrGroup the user or group to be set as owner.
   * @return user or group after transformation.
   * */
  public String transformUserOrGroupForSetRequest(String userOrGroup) {
    if (userOrGroup == null || userOrGroup.isEmpty() || skipUserIdentityReplacement) {
      return userOrGroup;
    }

    // case 1: when the owner to be set is stated in substitution list.
    if (isInSubstitutionList(userOrGroup)) {
      return servicePrincipalId;
    }

    // case 2: when the owner is a short name of the user principal name(UPN).
    if (shouldUseFullyQualifiedUserName(userOrGroup)) {
      return getFullyQualifiedName(userOrGroup);
    }

    return userOrGroup;
  }

  /**
   * Perform Identity transformation when calling setAcl(),removeAclEntries() and modifyAclEntries()
   * If the AclEntry type is a user or group, and its name is one of the following:
   * 1.short name; 2.$superuser; 3.Fully qualified name; 4. principal id.
   * Short name could be transformed to:
   *    - A service principal id or $superuser, if short name belongs a daemon service
   *      stated in substitution list AND "fs.azure.identity.transformer.service.principal.id"
   *      is set with $superuser or a principal id.
   *    - A fully qualified name, if the AclEntry type is User AND if "fs.azure.identity.transformer.domain.name"
   *    is set in configuration. This is to make the behavior consistent with HDI.
   *
   * $superuser, fully qualified name and principal id should not be transformed.
   *
   * @param aclEntries list of AclEntry
   * @return list of AclEntry after the identity transformation.
   * */
  public List<AclEntry> transformAclEntriesForSetRequest(final List<AclEntry> aclEntries) {
    if (skipUserIdentityReplacement) {
      return aclEntries;
    }

    for (int i = 0; i < aclEntries.size(); i++) {
      AclEntry aclEntry = aclEntries.get(i);
      String name = aclEntry.getName();
      String transformedName = name;
      if (name == null || name.isEmpty() || aclEntry.getType().equals(AclEntryType.OTHER) || aclEntry.getType().equals(AclEntryType.MASK)) {
        continue;
      }

      // case 1: when the user or group name to be set is stated in substitution list.
      if (isInSubstitutionList(name)) {
        transformedName = servicePrincipalId;
      } else if (aclEntry.getType().equals(AclEntryType.USER) // case 2: when the owner is a short name
              && shouldUseFullyQualifiedUserName(name)) {     //         of the user principal name (UPN).
        // Notice: for group type ACL entry, if name is shortName.
        //         It won't be converted to Full Name. This is
        //         to make the behavior consistent with HDI.
        transformedName = getFullyQualifiedName(name);
      }

      // Avoid unnecessary new AclEntry allocation
      if (transformedName.equals(name)) {
        continue;
      }

      AclEntry.Builder aclEntryBuilder = new AclEntry.Builder();
      aclEntryBuilder.setType(aclEntry.getType());
      aclEntryBuilder.setName(transformedName);
      aclEntryBuilder.setScope(aclEntry.getScope());
      aclEntryBuilder.setPermission(aclEntry.getPermission());

      // Replace the original AclEntry
      aclEntries.set(i, aclEntryBuilder.build());
    }
    return aclEntries;
  }

  /**
   * Internal method to identify if owner name returned by the ADLS backend is short name or not.
   * If name contains "@", this code assumes that whatever comes after '@' is domain name and ignores it.
   * @param owner
   * @return
   */
  private boolean isShortUserName(String owner) {
    return (owner != null) && !owner.contains(AT);
  }

  private boolean shouldUseShortUserName(String owner){
    return enableShortName && !isShortUserName(owner);
  }

  private String getShortName(String userName) {
    if (userName == null)    {
      return  null;
    }

    if (isShortUserName(userName)) {
      return userName;
    }

    String userNameBeforeAt = userName.substring(0, userName.indexOf(AT));
    if (isSecure) {
      //In secure clusters we apply auth to local rules to lowercase all short localUser names (notice /L at the end),
      //E.G. : RULE:[1:$1@$0](.*@FOO.ONMICROSOFT.COM)s/@.*/// Ideally we should use the HadoopKerberosName class to get
      // new HadoopKerberosName(arg).getShortName. However,
      //1. ADLS can report the Realm in lower case while returning file owner names( ie. : Some.User@realm.onmicrosoft.com)
      //2. The RULE specification does not allow specifying character classes to do case insensitive matches
      //Due to this, we end up using a forced lowercase version of the manually shortened name
      return userNameBeforeAt.toLowerCase(Locale.ENGLISH);
    }
    return userNameBeforeAt;
  }

  private String getFullyQualifiedName(String name){
    if (domainIsSet && (name != null) && !name.contains(AT)){
      return name + AT + domainName;
    }
    return name;
  }

  private boolean shouldUseFullyQualifiedUserName(String owner){
    return domainIsSet && !SUPER_USER.equals(owner) && !isUuid(owner) && enableShortName && isShortUserName(owner);
  }

  private boolean isInSubstitutionList(String localUserName) {
    return serviceWhiteList.contains(STAR) || serviceWhiteList.contains(localUserName);
  }

  private boolean isUuid(String input) {
    if (input == null) return false;
    return input.matches(UUID_PATTERN);
  }
}
