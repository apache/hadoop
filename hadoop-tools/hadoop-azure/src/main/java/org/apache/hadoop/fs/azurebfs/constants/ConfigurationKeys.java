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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Responsible to keep all the Azure Blob File System configurations keys in Hadoop configuration file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ConfigurationKeys {
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key";
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME_REGX = "fs\\.azure\\.account\\.key\\.(.*)";
  public static final String FS_AZURE_SECURE_MODE = "fs.azure.secure.mode";

  // Retry strategy defined by the user
  public static final String AZURE_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  public static final String AZURE_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  public static final String AZURE_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  public static final String AZURE_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";

  // Read and write buffer sizes defined by the user
  public static final String AZURE_WRITE_BUFFER_SIZE = "fs.azure.write.request.size";
  public static final String AZURE_READ_BUFFER_SIZE = "fs.azure.read.request.size";
  public static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  public static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME = "fs.azure.block.location.impersonatedhost";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentRequestCount.out";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentRequestCount.in";
  public static final String AZURE_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";
  public static final String AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION = "fs.azure.createRemoteFileSystemDuringInitialization";
  public static final String AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION = "fs.azure.skipUserGroupMetadataDuringInitialization";
  public static final String FS_AZURE_ENABLE_AUTOTHROTTLING = "fs.azure.enable.autothrottling";
  public static final String FS_AZURE_ALWAYS_USE_HTTPS = "fs.azure.always.use.https";
  public static final String FS_AZURE_ATOMIC_RENAME_KEY = "fs.azure.atomic.rename.key";
  public static final String FS_AZURE_READ_AHEAD_QUEUE_DEPTH = "fs.azure.readaheadqueue.depth";
  public static final String FS_AZURE_ENABLE_FLUSH = "fs.azure.enable.flush";
  public static final String FS_AZURE_USER_AGENT_PREFIX_KEY = "fs.azure.user.agent.prefix";
  public static final String FS_AZURE_SSL_CHANNEL_MODE_KEY = "fs.azure.ssl.channel.mode";
  public static final String FS_AZURE_USE_UPN = "fs.azure.use.upn";
  /** User principal names (UPNs) have the format “{alias}@{domain}”. If true,
   *  only {alias} is included when a UPN would otherwise appear in the output
   *  of APIs like getFileStatus, getOwner, getAclStatus, etc. Default is false. **/
  public static final String FS_AZURE_FILE_OWNER_ENABLE_SHORTNAME = "fs.azure.identity.transformer.enable.short.name";
  /** If the domain name is specified and “fs.azure.identity.transformer.enable.short.name”
   *  is true, then the {alias} part of a UPN can be specified as input to APIs like setOwner and
   *  setAcl and it will be transformed to a UPN by appending @ and the domain specified by
   *  this configuration property. **/
  public static final String FS_AZURE_FILE_OWNER_DOMAINNAME = "fs.azure.identity.transformer.domain.name";
  /** An Azure Active Directory object ID (oid) used as the replacement for names contained in the
   * list specified by “fs.azure.identity.transformer.service.principal.substitution.list.
   * Notice that instead of setting oid, you can also set $superuser.**/
  public static final String FS_AZURE_OVERRIDE_OWNER_SP = "fs.azure.identity.transformer.service.principal.id";
  /** A comma separated list of names to be replaced with the service principal ID specified by
   * “fs.default.identity.transformer.service.principal.id”. This substitution occurs
   * when setOwner, setAcl, modifyAclEntries, or removeAclEntries are invoked with identities
   * contained in the substitution list. Notice that when in non-secure cluster, asterisk symbol "*"
   * can be used to match all user/group. **/
  public static final String FS_AZURE_OVERRIDE_OWNER_SP_LIST = "fs.azure.identity.transformer.service.principal.substitution.list";
  /** By default this is set as false, so “$superuser” is replaced with the current user when it appears as the owner
   * or owning group of a file or directory. To disable it, set it as true. **/
  public static final String FS_AZURE_SKIP_SUPER_USER_REPLACEMENT = "fs.azure.identity.transformer.skip.superuser.replacement";
  public static final String AZURE_KEY_ACCOUNT_KEYPROVIDER = "fs.azure.account.keyprovider";
  public static final String AZURE_KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT = "fs.azure.shellkeyprovider.script";

  /** End point of ABFS account: {@value}. */
  public static final String AZURE_ABFS_ENDPOINT = "fs.azure.abfs.endpoint";
  /** Key for auth type properties: {@value}. */
  public static final String FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME = "fs.azure.account.auth.type";
  /** Key for oauth token provider type: {@value}. */
  public static final String FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME = "fs.azure.account.oauth.provider.type";
  /** Key for oauth AAD client id: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID = "fs.azure.account.oauth2.client.id";
  /** Key for oauth AAD client secret: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";
  /** Key for oauth AAD client endpoint: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT = "fs.azure.account.oauth2.client.endpoint";
  /** Key for oauth msi tenant id: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT = "fs.azure.account.oauth2.msi.tenant";
  /** Key for oauth user name: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_USER_NAME = "fs.azure.account.oauth2.user.name";
  /** Key for oauth user password: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD = "fs.azure.account.oauth2.user.password";
  /** Key for oauth refresh token: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN = "fs.azure.account.oauth2.refresh.token";

  public static String accountProperty(String property, String account) {
    return property + "." + account;
  }

  public static final String FS_AZURE_ENABLE_DELEGATION_TOKEN = "fs.azure.enable.delegation.token";
  public static final String FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE = "fs.azure.delegation.token.provider.type";

  public static final String ABFS_EXTERNAL_AUTHORIZATION_CLASS = "abfs.external.authorization.class";

  private ConfigurationKeys() {}
}
