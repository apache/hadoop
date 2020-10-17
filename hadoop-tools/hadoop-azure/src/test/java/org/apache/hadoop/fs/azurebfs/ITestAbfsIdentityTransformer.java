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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.permission.AclEntry;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SUPER_USER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_FILE_OWNER_DOMAINNAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_OVERRIDE_OWNER_SP;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_OVERRIDE_OWNER_SP_LIST;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SKIP_SUPER_USER_REPLACEMENT;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;

/**
 * Test IdentityTransformer.
 */
//@RunWith(Parameterized.class)
public class ITestAbfsIdentityTransformer extends AbstractAbfsScaleTest{
  private final UserGroupInformation userGroupInfo;
  private final String localUser;
  private final String localGroup;
  private static final String DAEMON = "daemon";
  private static final String ASTERISK = "*";
  private static final String SHORT_NAME = "abc";
  private static final String DOMAIN = "domain.com";
  private static final String FULLY_QUALIFIED_NAME = "abc@domain.com";
  private static final String SERVICE_PRINCIPAL_ID = UUID.randomUUID().toString();

  public ITestAbfsIdentityTransformer() throws Exception {
    super();
    userGroupInfo = UserGroupInformation.getCurrentUser();
    localUser = userGroupInfo.getShortUserName();
    localGroup = userGroupInfo.getPrimaryGroupName();
  }

  @Test
  public void testDaemonServiceSettingIdentity() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);
    // Default config
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    assertEquals("Identity should not change for default config",
            DAEMON, identityTransformer.transformUserOrGroupForSetRequest(DAEMON));

    // Add service principal id
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, SERVICE_PRINCIPAL_ID);

    // case 1: substitution list doesn't contain daemon
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, "a,b,c,d");
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("Identity should not change when substitution list doesn't contain daemon",
            DAEMON, identityTransformer.transformUserOrGroupForSetRequest(DAEMON));

    // case 2: substitution list contains daemon name
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, DAEMON + ",a,b,c,d");
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("Identity should be replaced to servicePrincipalId",
            SERVICE_PRINCIPAL_ID, identityTransformer.transformUserOrGroupForSetRequest(DAEMON));

    // case 3: substitution list is *
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, ASTERISK);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("Identity should be replaced to servicePrincipalId",
            SERVICE_PRINCIPAL_ID, identityTransformer.transformUserOrGroupForSetRequest(DAEMON));
  }

  @Test
  public void testFullyQualifiedNameSettingIdentity() throws IOException {
    Configuration config = this.getRawConfiguration();
    // Default config
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    assertEquals("short name should not be converted to full name by default",
            SHORT_NAME, identityTransformer.transformUserOrGroupForSetRequest(SHORT_NAME));

    resetIdentityConfig(config);

    // Add config to get fully qualified username
    config.setBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, true);
    config.set(FS_AZURE_FILE_OWNER_DOMAINNAME, DOMAIN);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("short name should be converted to full name",
            FULLY_QUALIFIED_NAME, identityTransformer.transformUserOrGroupForSetRequest(SHORT_NAME));
  }

  @Test
  public void testNoOpForSettingOidAsIdentity() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    config.setBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, true);
    config.set(FS_AZURE_FILE_OWNER_DOMAINNAME, DOMAIN);
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, UUID.randomUUID().toString());
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, "a,b,c,d");

    IdentityTransformer identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    final String principalId = UUID.randomUUID().toString();
    assertEquals("Identity should not be changed when owner is already a principal id ",
            principalId, identityTransformer.transformUserOrGroupForSetRequest(principalId));
  }

  @Test
  public void testNoOpWhenSettingSuperUserAsdentity() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    config.setBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, true);
    config.set(FS_AZURE_FILE_OWNER_DOMAINNAME, DOMAIN);
    // Default config
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    assertEquals("Identity should not be changed because it is not in substitution list",
            SUPER_USER, identityTransformer.transformUserOrGroupForSetRequest(SUPER_USER));
  }

  @Test
  public void testIdentityReplacementForSuperUserGetRequest() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    // with default config, identityTransformer should do $superUser replacement
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    assertEquals("$superuser should be replaced with local user by default",
            localUser, identityTransformer.transformIdentityForGetRequest(SUPER_USER, true, localUser));

    // Disable $supeuser replacement
    config.setBoolean(FS_AZURE_SKIP_SUPER_USER_REPLACEMENT, true);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("$superuser should not be replaced",
            SUPER_USER, identityTransformer.transformIdentityForGetRequest(SUPER_USER, true, localUser));
  }

  @Test
  public void testIdentityReplacementForDaemonServiceGetRequest() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    // Default config
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    assertEquals("By default servicePrincipalId should not be converted for GetFileStatus(), listFileStatus(), getAcl()",
            SERVICE_PRINCIPAL_ID, identityTransformer.transformIdentityForGetRequest(SERVICE_PRINCIPAL_ID, true, localUser));

    resetIdentityConfig(config);
    // 1. substitution list doesn't contain currentUser
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, "a,b,c,d");
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("servicePrincipalId should not be replaced if local daemon user is not in substitution list",
            SERVICE_PRINCIPAL_ID, identityTransformer.transformIdentityForGetRequest(SERVICE_PRINCIPAL_ID, true, localUser));

    resetIdentityConfig(config);
    // 2. substitution list contains currentUser(daemon name) but the service principal id in config doesn't match
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, localUser + ",a,b,c,d");
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, UUID.randomUUID().toString());
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("servicePrincipalId should not be replaced if it is not equal to the SPN set in config",
            SERVICE_PRINCIPAL_ID, identityTransformer.transformIdentityForGetRequest(SERVICE_PRINCIPAL_ID, true, localUser));

    resetIdentityConfig(config);
    // 3. substitution list contains currentUser(daemon name) and the service principal id in config matches
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, localUser + ",a,b,c,d");
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, SERVICE_PRINCIPAL_ID);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("servicePrincipalId should be transformed to local use",
            localUser, identityTransformer.transformIdentityForGetRequest(SERVICE_PRINCIPAL_ID, true, localUser));

    resetIdentityConfig(config);
    // 4. substitution is "*" but the service principal id in config doesn't match the input
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, ASTERISK);
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, UUID.randomUUID().toString());
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("servicePrincipalId should not be replaced if it is not equal to the SPN set in config",
            SERVICE_PRINCIPAL_ID, identityTransformer.transformIdentityForGetRequest(SERVICE_PRINCIPAL_ID, true, localUser));

    resetIdentityConfig(config);
    // 5. substitution is "*" and the service principal id in config match the input
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, ASTERISK);
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, SERVICE_PRINCIPAL_ID);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("servicePrincipalId should be transformed to local user",
            localUser, identityTransformer.transformIdentityForGetRequest(SERVICE_PRINCIPAL_ID, true, localUser));
  }

  @Test
  public void testIdentityReplacementForKinitUserGetRequest() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    // Default config
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    assertEquals("full name should not be transformed if shortname is not enabled",
            FULLY_QUALIFIED_NAME, identityTransformer.transformIdentityForGetRequest(FULLY_QUALIFIED_NAME, true, localUser));

    // add config to get short name
    config.setBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, true);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);
    assertEquals("should convert the full owner name to shortname ",
            SHORT_NAME, identityTransformer.transformIdentityForGetRequest(FULLY_QUALIFIED_NAME, true, localUser));

    assertEquals("group name should not be converted to shortname ",
            FULLY_QUALIFIED_NAME, identityTransformer.transformIdentityForGetRequest(FULLY_QUALIFIED_NAME, false, localGroup));
  }

  @Test
  public void transformAclEntriesForSetRequest() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    List<AclEntry> aclEntriesToBeTransformed = Lists.newArrayList(
            aclEntry(ACCESS, USER, DAEMON, ALL),
            aclEntry(ACCESS, USER, FULLY_QUALIFIED_NAME,ALL),
            aclEntry(DEFAULT, USER, SUPER_USER, ALL),
            aclEntry(DEFAULT, USER, SERVICE_PRINCIPAL_ID, ALL),
            aclEntry(DEFAULT, USER, SHORT_NAME, ALL),
            aclEntry(DEFAULT, GROUP, DAEMON, ALL),
            aclEntry(DEFAULT, GROUP, SHORT_NAME, ALL), // Notice: for group type ACL entry, if name is shortName,
            aclEntry(DEFAULT, OTHER, ALL),             //         It won't be converted to Full Name. This is
            aclEntry(DEFAULT, MASK, ALL)               //         to make the behavior consistent with HDI.
    );

    // make a copy
    List<AclEntry> aclEntries = Lists.newArrayList(aclEntriesToBeTransformed);

    // Default config should not change the identities
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    identityTransformer.transformAclEntriesForSetRequest(aclEntries);
    checkAclEntriesList(aclEntriesToBeTransformed, aclEntries);

    resetIdentityConfig(config);
    // With config
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, DAEMON + ",a,b,c,d");
    config.setBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, true);
    config.set(FS_AZURE_FILE_OWNER_DOMAINNAME, DOMAIN);
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, SERVICE_PRINCIPAL_ID);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);

    identityTransformer.transformAclEntriesForSetRequest(aclEntries);

    // expected acl entries
    List<AclEntry> expectedAclEntries = Lists.newArrayList(
            aclEntry(ACCESS, USER, SERVICE_PRINCIPAL_ID, ALL),
            aclEntry(ACCESS, USER, FULLY_QUALIFIED_NAME, ALL),
            aclEntry(DEFAULT, USER, SUPER_USER, ALL),
            aclEntry(DEFAULT, USER, SERVICE_PRINCIPAL_ID, ALL),
            aclEntry(DEFAULT, USER, FULLY_QUALIFIED_NAME, ALL),
            aclEntry(DEFAULT, GROUP, SERVICE_PRINCIPAL_ID, ALL),
            aclEntry(DEFAULT, GROUP, SHORT_NAME, ALL),
            aclEntry(DEFAULT, OTHER, ALL),
            aclEntry(DEFAULT, MASK, ALL)
    );

    checkAclEntriesList(aclEntries, expectedAclEntries);
  }

  @Test
  public void transformAclEntriesForGetRequest() throws IOException {
    Configuration config = this.getRawConfiguration();
    resetIdentityConfig(config);

    List<AclEntry> aclEntriesToBeTransformed = Lists.newArrayList(
            aclEntry(ACCESS, USER, FULLY_QUALIFIED_NAME, ALL),
            aclEntry(DEFAULT, USER, SUPER_USER, ALL),
            aclEntry(DEFAULT, USER, SERVICE_PRINCIPAL_ID, ALL),
            aclEntry(DEFAULT, USER, SHORT_NAME, ALL),
            aclEntry(DEFAULT, GROUP, SHORT_NAME, ALL),
            aclEntry(DEFAULT, OTHER, ALL),
            aclEntry(DEFAULT, MASK, ALL)
    );

    // make a copy
    List<AclEntry> aclEntries = Lists.newArrayList(aclEntriesToBeTransformed);

    // Default config should not change the identities
    IdentityTransformer identityTransformer = getTransformerWithDefaultIdentityConfig(config);
    identityTransformer.transformAclEntriesForGetRequest(aclEntries, localUser, localGroup);
    checkAclEntriesList(aclEntriesToBeTransformed, aclEntries);

    resetIdentityConfig(config);
    // With config
    config.set(FS_AZURE_OVERRIDE_OWNER_SP_LIST, localUser + ",a,b,c,d");
    config.setBoolean(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME, true);
    config.set(FS_AZURE_FILE_OWNER_DOMAINNAME, DOMAIN);
    config.set(FS_AZURE_OVERRIDE_OWNER_SP, SERVICE_PRINCIPAL_ID);
    identityTransformer = getTransformerWithCustomizedIdentityConfig(config);

    // make a copy
    aclEntries = Lists.newArrayList(aclEntriesToBeTransformed);
    identityTransformer.transformAclEntriesForGetRequest(aclEntries, localUser, localGroup);

    // expected acl entries
    List<AclEntry> expectedAclEntries = Lists.newArrayList(
            aclEntry(ACCESS, USER, SHORT_NAME, ALL), // Full UPN should be transformed to shortName
            aclEntry(DEFAULT, USER, localUser, ALL), // $SuperUser should be transformed to shortName
            aclEntry(DEFAULT, USER, localUser, ALL), // principal Id should be transformed to local user name
            aclEntry(DEFAULT, USER, SHORT_NAME, ALL),
            aclEntry(DEFAULT, GROUP, SHORT_NAME, ALL),
            aclEntry(DEFAULT, OTHER, ALL),
            aclEntry(DEFAULT, MASK, ALL)
    );

    checkAclEntriesList(aclEntries, expectedAclEntries);
  }

  private void resetIdentityConfig(Configuration config) {
    config.unset(FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME);
    config.unset(FS_AZURE_FILE_OWNER_DOMAINNAME);
    config.unset(FS_AZURE_OVERRIDE_OWNER_SP);
    config.unset(FS_AZURE_OVERRIDE_OWNER_SP_LIST);
    config.unset(FS_AZURE_SKIP_SUPER_USER_REPLACEMENT);
  }

  private IdentityTransformer getTransformerWithDefaultIdentityConfig(Configuration config) throws IOException {
    resetIdentityConfig(config);
    return new IdentityTransformer(config);
  }

  private IdentityTransformer getTransformerWithCustomizedIdentityConfig(Configuration config) throws IOException {
    return new IdentityTransformer(config);
  }

  private void checkAclEntriesList(List<AclEntry> aclEntries, List<AclEntry> expected) {
    assertTrue("list size not equals", aclEntries.size() == expected.size());
    for (int i = 0; i < aclEntries.size(); i++) {
      assertEquals("Identity doesn't match", expected.get(i).getName(), aclEntries.get(i).getName());
    }
  }
}
