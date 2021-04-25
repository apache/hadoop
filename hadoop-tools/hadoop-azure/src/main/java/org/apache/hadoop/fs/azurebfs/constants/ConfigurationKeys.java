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
import org.apache.hadoop.fs.FileSystem;

/**
 * Responsible to keep all the Azure Blob File System configurations keys in Hadoop configuration file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ConfigurationKeys {

  /**
   * Config to specify if the configured account is HNS enabled or not. If
   * this config is not set, getacl call is made on account filesystem root
   * path to determine HNS status.
   */
  public static final String FS_AZURE_ACCOUNT_IS_HNS_ENABLED = "fs.azure.account.hns.enabled";
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key";
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME_REGX = "fs\\.azure\\.account\\.key\\.(.*)";
  public static final String FS_AZURE_SECURE_MODE = "fs.azure.secure.mode";

  // Retry strategy defined by the user
  public static final String AZURE_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  public static final String AZURE_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  public static final String AZURE_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  public static final String AZURE_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";
  public static final String AZURE_CUSTOM_TOKEN_FETCH_RETRY_COUNT = "fs.azure.custom.token.fetch.retry.count";

  //  Retry strategy for getToken calls
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_COUNT = "fs.azure.oauth.token.fetch.retry.max.retries";
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF = "fs.azure.oauth.token.fetch.retry.min.backoff.interval";
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF = "fs.azure.oauth.token.fetch.retry.max.backoff.interval";
  public static final String AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF = "fs.azure.oauth.token.fetch.retry.delta.backoff";

  // Read and write buffer sizes defined by the user
  public static final String AZURE_WRITE_MAX_CONCURRENT_REQUESTS = "fs.azure.write.max.concurrent.requests";
  public static final String AZURE_WRITE_MAX_REQUESTS_TO_QUEUE = "fs.azure.write.max.requests.to.queue";
  public static final String AZURE_WRITE_BUFFER_SIZE = "fs.azure.write.request.size";
  /** If the data size written by Hadoop app is small, i.e. data size :
   *  (a) before any of HFlush/HSync call is made or
   *  (b) between 2 HFlush/Hsync API calls
   *  is less than write buffer size, 2 separate calls, one for append and
   *  another for flush are made.
   *  By enabling the small write optimization, a single call will be made to
   *  perform both append and flush operations and hence reduce request count.
   */
  public static final String AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION = "fs.azure.write.enableappendwithflush";
  public static final String AZURE_READ_BUFFER_SIZE = "fs.azure.read.request.size";
  public static final String AZURE_READ_SMALL_FILES_COMPLETELY = "fs.azure.read.smallfilescompletely";
  public static final String AZURE_READ_OPTIMIZE_FOOTER_READ = "fs.azure.read.optimizefooterread";
  public static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  public static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME = "fs.azure.block.location.impersonatedhost";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentRequestCount.out";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentRequestCount.in";
  public static final String AZURE_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";
  public static final String AZURE_LIST_MAX_RESULTS = "fs.azure.list.max.results";
  public static final String AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION = "fs.azure.createRemoteFileSystemDuringInitialization";
  public static final String AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION = "fs.azure.skipUserGroupMetadataDuringInitialization";
  public static final String FS_AZURE_ENABLE_AUTOTHROTTLING = "fs.azure.enable.autothrottling";
  public static final String FS_AZURE_ALWAYS_USE_HTTPS = "fs.azure.always.use.https";
  public static final String FS_AZURE_ATOMIC_RENAME_KEY = "fs.azure.atomic.rename.key";
  /** This config ensures that during create overwrite an existing file will be
   *  overwritten only if there is a match on the eTag of existing file.
   */
  public static final String FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE = "fs.azure.enable.conditional.create.overwrite";
  public static final String FS_AZURE_ENABLE_MKDIR_OVERWRITE = "fs.azure.enable.mkdir.overwrite";
  /** Provides a config to provide comma separated path prefixes on which Appendblob based files are created
   *  Default is empty. **/
  public static final String FS_AZURE_APPEND_BLOB_KEY = "fs.azure.appendblob.directories";
  /** Provides a config to provide comma separated path prefixes which support infinite leases.
   *  Files under these paths will be leased when created or opened for writing and the lease will
   *  be released when the file is closed. The lease may be broken with the breakLease method on
   *  AzureBlobFileSystem. Default is empty.
   * **/
  public static final String FS_AZURE_INFINITE_LEASE_KEY = "fs.azure.infinite-lease.directories";
  /** Provides a number of threads to use for lease operations for infinite lease directories.
   *  Must be set to a minimum of 1 if infinite lease directories are to be used. Default is 0. **/
  public static final String FS_AZURE_LEASE_THREADS = "fs.azure.lease.threads";
  public static final String FS_AZURE_READ_AHEAD_QUEUE_DEPTH = "fs.azure.readaheadqueue.depth";
  public static final String FS_AZURE_ALWAYS_READ_BUFFER_SIZE = "fs.azure.read.alwaysReadBufferSize";
  public static final String FS_AZURE_READ_AHEAD_BLOCK_SIZE = "fs.azure.read.readahead.blocksize";
  /** Provides a config control to enable or disable ABFS Flush operations -
   *  HFlush and HSync. Default is true. **/
  public static final String FS_AZURE_ENABLE_FLUSH = "fs.azure.enable.flush";
  /** Provides a config control to disable or enable OutputStream Flush API
   *  operations in AbfsOutputStream. Flush() will trigger actions that
   *  guarantee that buffered data is persistent with a perf cost while the API
   *  documentation does not have such expectations of data being persisted.
   *  Default value of this config is true. **/
  public static final String FS_AZURE_DISABLE_OUTPUTSTREAM_FLUSH = "fs.azure.disable.outputstream.flush";
  public static final String FS_AZURE_USER_AGENT_PREFIX_KEY = "fs.azure.user.agent.prefix";
  public static final String FS_AZURE_CLUSTER_NAME = "fs.azure.cluster.name";
  public static final String FS_AZURE_CLUSTER_TYPE = "fs.azure.cluster.type";
  public static final String FS_AZURE_SSL_CHANNEL_MODE_KEY = "fs.azure.ssl.channel.mode";
  /** Provides a config to enable/disable the checkAccess API.
   *  By default this will be
   *  FileSystemConfigurations.DEFAULT_ENABLE_CHECK_ACCESS. **/
  public static final String FS_AZURE_ENABLE_CHECK_ACCESS = "fs.azure.enable.check.access";
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
  /** Setting this true will make the driver use it's own RemoteIterator implementation */
  public static final String FS_AZURE_ENABLE_ABFS_LIST_ITERATOR = "fs.azure.enable.abfslistiterator";
  /** Server side encryption key */
  public static final String FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY = "fs.azure.client-provided-encryption-key";

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
  /** Key for oauth msi endpoint: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT = "fs.azure.account.oauth2.msi.endpoint";
  /** Key for oauth msi Authority: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY = "fs.azure.account.oauth2.msi.authority";
  /** Key for oauth user name: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_USER_NAME = "fs.azure.account.oauth2.user.name";
  /** Key for oauth user password: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD = "fs.azure.account.oauth2.user.password";
  /** Key for oauth refresh token: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN = "fs.azure.account.oauth2.refresh.token";
  /** Key for oauth AAD refresh token endpoint: {@value}. */
  public static final String FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN_ENDPOINT = "fs.azure.account.oauth2.refresh.token.endpoint";
  /** Key for enabling the tracking of ABFS API latency and sending the latency numbers to the ABFS API service */
  public static final String FS_AZURE_ABFS_LATENCY_TRACK = "fs.azure.abfs.latency.track";

  public static String accountProperty(String property, String account) {
    return property + "." + account;
  }

  public static final String FS_AZURE_ENABLE_DELEGATION_TOKEN = "fs.azure.enable.delegation.token";
  public static final String FS_AZURE_DELEGATION_TOKEN_PROVIDER_TYPE = "fs.azure.delegation.token.provider.type";

  /** Key for SAS token provider **/
  public static final String FS_AZURE_SAS_TOKEN_PROVIDER_TYPE = "fs.azure.sas.token.provider.type";

  /** For performance, AbfsInputStream/AbfsOutputStream re-use SAS tokens until the expiry is within this number of seconds. **/
  public static final String FS_AZURE_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS = "fs.azure.sas.token.renew.period.for.streams";

  /** Key to enable custom identity transformation. */
  public static final String FS_AZURE_IDENTITY_TRANSFORM_CLASS = "fs.azure.identity.transformer.class";
  /** Key for Local User to Service Principal file location. */
  public static final String FS_AZURE_LOCAL_USER_SP_MAPPING_FILE_PATH = "fs.azure.identity.transformer.local.service.principal.mapping.file.path";
  /** Key for Local Group to Service Group file location. */
  public static final String FS_AZURE_LOCAL_GROUP_SG_MAPPING_FILE_PATH = "fs.azure.identity.transformer.local.service.group.mapping.file.path";
  /**
   * Optional config to enable a lock free pread which will bypass buffer in AbfsInputStream.
   * This is not a config which can be set at cluster level. It can be used as
   * an option on FutureDataInputStreamBuilder.
   * @see FileSystem#openFile(org.apache.hadoop.fs.Path)
   */
  public static final String FS_AZURE_BUFFERED_PREAD_DISABLE = "fs.azure.buffered.pread.disable";
  private ConfigurationKeys() {}
}
