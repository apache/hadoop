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

/**
 * Responsible to keep all the Azure Blob File System configurations keys in Hadoop configuration file.
 */
public final class TestConfigurationKeys {
  public static final String FS_AZURE_ACCOUNT_NAME = "fs.azure.account.name";
  public static final String FS_AZURE_ABFS_ACCOUNT_NAME = "fs.azure.abfs.account.name";
  public static final String FS_AZURE_ACCOUNT_KEY = "fs.azure.account.key";
  public static final String FS_AZURE_CONTRACT_TEST_URI = "fs.contract.test.fs.abfs";
  public static final String FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT = "fs.azure.test.namespace.enabled";
  public static final String FS_AZURE_TEST_APPENDBLOB_ENABLED = "fs.azure.test.appendblob.enabled";
  public static final String FS_AZURE_TEST_CPK_ENABLED = "fs.azure.test.cpk.enabled";

  public static final String FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID = "fs.azure.account.oauth2.contributor.client.id";
  public static final String FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET = "fs.azure.account.oauth2.contributor.client.secret";

  public static final String FS_AZURE_BLOB_DATA_READER_CLIENT_ID = "fs.azure.account.oauth2.reader.client.id";
  public static final String FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET = "fs.azure.account.oauth2.reader.client.secret";

  public static final String FS_AZURE_BLOB_FS_CLIENT_ID = "fs.azure.account.oauth2.client.id";
  public static final String FS_AZURE_BLOB_FS_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";

  public static final String FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID = "fs.azure.account.test.oauth2.client.id";
  public static final String FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET = "fs.azure.account.test.oauth2.client.secret";

  public static final String FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID = "fs.azure.check.access.testuser.guid";

  public static final String MOCK_SASTOKENPROVIDER_FAIL_INIT = "mock.sastokenprovider.fail.init";
  public static final String MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN = "mock.sastokenprovider.return.empty.sasToken";

  public static final String FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID = "fs.azure.test.app.service.principal.tenant.id";

  public static final String FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID = "fs.azure.test.app.service.principal.object.id";

  public static final String FS_AZURE_TEST_APP_ID = "fs.azure.test.app.id";

  public static final String FS_AZURE_TEST_APP_SECRET = "fs.azure.test.app.secret";

  public static final String FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT = "fs.azure.test.cpk-enabled-secondary-account";
  public static final String FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT_KEY = "fs.azure.test.cpk-enabled-secondary-account.key";

  public static final String TEST_CONFIGURATION_FILE_NAME = "azure-test.xml";
  public static final String TEST_CONTAINER_PREFIX = "abfs-testcontainer-";
  public static final int TEST_TIMEOUT = 15 * 60 * 1000;

  private TestConfigurationKeys() {}
}
