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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.util.Lists;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAbfsPaginatedDelete extends AbstractAbfsIntegrationTest {

    private AzureBlobFileSystem superUserFs;
    private AzureBlobFileSystem firstTestUserFs;
    private String firstTestUserGuid;

    private boolean isHnsEnabled;
    public ITestAbfsPaginatedDelete() throws Exception {
    }

    @Override
    public void setup() throws Exception {
        isHnsEnabled = this.getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
        loadConfiguredFileSystem();
        super.setup();
        this.superUserFs = getFileSystem();
        this.firstTestUserGuid = getConfiguration()
                .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);

        if(isHnsEnabled) {
            // setting up ACL permissions for test user
            setFirstTestUserFsAuth();
            setDefaultAclOnRoot(this.firstTestUserGuid);
        }

    }

    @Test
    public void testFnsDeleteWithPaginationTrue() throws Exception {
        Assume.assumeFalse(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = getFileSystem().getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        abfsConfig.setBoolean(FS_AZURE_ENABLE_PAGINATED_DELETE, true);

        TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);
        finalClient.deletePath(smallDirPath.toString(), true, null, testTracingContext);

        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.getPathStatus(smallDirPath.toString(), false, testTracingContext));
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, e.getStatusCode());
    }

    @Test
    public void testFnsDeleteWithPaginationFalse() throws Exception {
        Assume.assumeFalse(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = getFileSystem().getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        abfsConfig.setBoolean(FS_AZURE_ENABLE_PAGINATED_DELETE, true);

        TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);
        finalClient.deletePath(smallDirPath.toString(), true, null, testTracingContext);

        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.getPathStatus(smallDirPath.toString(), false, testTracingContext));
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, e.getStatusCode());
    }

    @Test
    public void testVersionForPagination() throws Exception {
        Assume.assumeTrue(isHnsEnabled);
        Path smallDirPath = createSmallDir();
        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        AbfsConfiguration abfsConfig = client.getAbfsConfiguration();

        // delete should fail with bad request as version does not support pagination
        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                client.deletePath(smallDirPath.toString(), true, null, getTestTracingContext(this.firstTestUserFs, false))
        );
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testInvalidPaginationTrueRecursiveFalse() throws Exception {
        Assume.assumeTrue(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);

        // delete should fail with HTTP as recursive will be set to false
        // but pagination parameter is set to true
        String path = smallDirPath.toString();
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.deletePath(path, false, null, getTestTracingContext(this.firstTestUserFs, false)));
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testInvalidPaginationFalseRandomCt() throws Exception {
        Assume.assumeTrue(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, false);
        String ct = "randomToken12345";
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.deletePath(smallDirPath.toString(), true, ct, getTestTracingContext(this.firstTestUserFs, false))
        );

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testInvalidRecursiveFalseRandomCt() throws Exception {
        Assume.assumeTrue(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);
        String ct = "randomToken12345";
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.deletePath(smallDirPath.toString(), false, ct, getTestTracingContext(this.firstTestUserFs, false))
        );

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testInvalidRecursiveFalsePaginationTrue() throws Exception {
        Assume.assumeTrue(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.deletePath(smallDirPath.toString(), false, null, getTestTracingContext(this.firstTestUserFs, false))
        );
    }

    @Test
    public void testValidRecursiveTruePaginationFalse() throws Exception {
        Assume.assumeTrue(isHnsEnabled);
        Path smallDirPath = createSmallDir();

        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        AbfsClient finalClient = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2023-08-03");
        AbfsConfiguration abfsConfig = finalClient.getAbfsConfiguration();
        TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, false);
        finalClient.deletePath(smallDirPath.toString(), true, null, testTracingContext);

        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                finalClient.getPathStatus(smallDirPath.toString(), false, testTracingContext));
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, e.getStatusCode());
    }

    private void setFirstTestUserFsAuth() throws IOException {
        if (this.firstTestUserFs != null) {
            return;
        }
        checkIfConfigIsSet(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT
                + "." + getAccountName());
        Configuration conf = getRawConfiguration();
        setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_ID, FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID);
        setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_SECRET,
                FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET);
        conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.OAuth.name());
        conf.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + "."
                + getAccountName(), ClientCredsTokenProvider.class.getName());
        conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
                false);
        this.firstTestUserFs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    }


    private void setTestFsConf(final String fsConfKey,
                               final String testFsConfKey) {
        final String confKeyWithAccountName = fsConfKey + "." + getAccountName();
        final String confValue = getConfiguration()
                .getString(testFsConfKey, "");
        getRawConfiguration().set(confKeyWithAccountName, confValue);
    }

    private void setDefaultAclOnRoot(String uid)
            throws IOException {
        List<AclEntry> aclSpec =  Lists.newArrayList(AclTestHelpers
                        .aclEntry(AclEntryScope.ACCESS, AclEntryType.USER, uid, FsAction.ALL),
                AclTestHelpers.aclEntry(AclEntryScope.DEFAULT, AclEntryType.USER, uid, FsAction.ALL));
        this.superUserFs.modifyAclEntries(new Path("/"), aclSpec);
    }

    private String getContinuationToken(AbfsHttpOperation resultOp) {
        String continuation = resultOp.getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        return continuation;
    }


    private Path createLargeDir() throws IOException {
        AzureBlobFileSystem fs = getFileSystem();
        String rootPath = "/largeDir";
        String firstFilePath = rootPath + "/placeholderFile";
        fs.create(new Path(firstFilePath));

        for (int i = 1; i <= 515; i++) {
            String dirPath = "/dirLevel1" + String.valueOf(i) + "/dirLevel2" + String.valueOf(i);
            String filePath = rootPath + dirPath + "/file" + String.valueOf(i);
            fs.create(new Path(filePath));
        }
        return new Path(rootPath);
    }

    private Path createSmallDir() throws IOException {
        String rootPath = "/smallDir";
        String firstFilePath = rootPath + "/placeholderFile";
        this.superUserFs.create(new Path(firstFilePath));

        for (int i = 1; i <= 2; i++) {
            String dirPath = "/dirLevel1-" + i + "/dirLevel2-" + i;
            String filePath = rootPath + dirPath + "/file-" + i;
            this.superUserFs.create(new Path(filePath));
        }
        return new Path(rootPath);
    }

    private void checkIfConfigIsSet(String configKey){
        AbfsConfiguration conf = getConfiguration();
        String value = conf.get(configKey);
        Assume.assumeTrue(configKey + " config is mandatory for the test to run",
                value != null && value.trim().length() > 1);
    }
}
