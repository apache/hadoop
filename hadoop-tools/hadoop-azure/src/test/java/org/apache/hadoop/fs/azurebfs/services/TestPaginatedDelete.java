package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.util.Lists;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.assertj.core.api.Assertions;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestPaginatedDelete extends AbstractAbfsIntegrationTest {

    private long knownPageSize;
    private FileSystem superUserFs;
    private AzureBlobFileSystem firstTestUserFs;
    private FileSystem secondTestUserFs;
    private String firstTestUserGuid;
    private String secondTestUserGuid;

    public TestPaginatedDelete() throws Exception {
        super.setup();
        loadConfiguredFileSystem();
        boolean isHnsEnabled = this.getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
        Assume.assumeTrue(isHnsEnabled);
        this.knownPageSize = 1024; // value set based on current DC limit on tenant
        this.superUserFs = getFileSystem();
        this.firstTestUserGuid = getConfiguration()
                .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);
        this.secondTestUserGuid = getConfiguration()
                .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_2_GUID);
        setFirstTestUserFsAuth();
        setSecondTestUserFsAuth();
    }

    private void setFirstTestUserFsAuth() throws IOException {
        if (this.firstTestUserFs != null) {
            return;
        }
        checkIfConfigIsSet(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT
                + "." + getAccountName());
        Configuration conf = getRawConfiguration();
        setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_ID,
                FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID);
        setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_SECRET,
                FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET);
        conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.OAuth.name());
        conf.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + "."
                + getAccountName(), ClientCredsTokenProvider.class.getName());
        conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
                false);
        this.firstTestUserFs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    }

    private void setSecondTestUserFsAuth() throws IOException {
        if (this.secondTestUserFs != null) {
            return;
        }
        checkIfConfigIsSet(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT
                + "." + getAccountName());
        Configuration conf = getRawConfiguration();
        setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_ID,
                FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID_2);
        setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_SECRET,
                FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET_2);
        conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.OAuth.name());
        conf.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + "."
                + getAccountName(), ClientCredsTokenProvider.class.getName());
        conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
                false);
        this.secondTestUserFs = FileSystem.newInstance(getRawConfiguration());
    }

    private void setTestFsConf(final String fsConfKey,
                               final String testFsConfKey) {
        final String confKeyWithAccountName = fsConfKey + "." + getAccountName();
        final String confValue = getConfiguration()
                .getString(testFsConfKey, "");
        getRawConfiguration().set(confKeyWithAccountName, confValue);
    }

    private void setDefaultAclOnRoot(Path parentDir, String uid)
            throws IOException {
        List<AclEntry> aclSpec =  Lists.newArrayList(AclTestHelpers
                .aclEntry(AclEntryScope.ACCESS, AclEntryType.USER, uid, FsAction.ALL),
                AclTestHelpers.aclEntry(AclEntryScope.DEFAULT, AclEntryType.USER, uid,
                        FsAction.ALL));
        this.superUserFs.modifyAclEntries(parentDir, aclSpec);
    }

    @Test
    public void testNormalPaginationBehavior() {

    }
    @Test
    public void testVersionForPagination() throws Exception {
        Path smallDirPath = createSmallDir();
        // setting defaultAcl on root for the first User
        setDefaultAclOnRoot(smallDirPath, this.firstTestUserGuid);

        AbfsConfiguration abfsConfig = getConfiguration(); // update to retrieve fixed test configs
        AbfsClient client = this.firstTestUserFs.getAbfsStore().getClient();
        client.deletePath(smallDirPath.toString(), true, null, getTestTracingContext(this.firstTestUserFs, false));
//        client = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2021-12-02");
//        AbfsClient finalClient = client;
//
//        String path = "/smallDir";
//        finalClient.deletePath(path, true, null, getTestTracingContext(fs, false));
//        // delete should fail with bad request as version does not support pagination
//        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);
//        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
//                finalClient.deletePath(path, true, null, getTestTracingContext(fs, false))
//        );
//        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
//
//        // delete should fail again irrespective of pagination parameter value
//        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, false);
//        e = intercept(AbfsRestOperationException.class, () ->
//                finalClient.deletePath(path, true, null, getTestTracingContext(fs, false))
//        );
//        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testInvalidPaginationTrueRecursiveFalse() throws Exception {
        Path smallDirPath = createSmallDir();
        // setting defaultAcl on root for the first User
        setDefaultAclOnRoot(smallDirPath, this.firstTestUserGuid);
        AbfsConfiguration abfsConfig = getConfiguration(); // update to retrieve fixed test configs
        AzureBlobFileSystem fs = getFileSystem(); // update to retrieve fixed test configs
        AbfsClient client = fs.getAbfsStore().getClient();

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);

        // delete should fail with bad request as recursive will be set to false
        // but pagination parameter is set to true
        String path = smallDirPath.toString();
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                client.deletePath(path, false, null, getTestTracingContext(fs, false)));
        assertEquals(HttpURLConnection.HTTP_CONFLICT, e.getStatusCode());
    }

    @Test
    public void testInvalidPaginationFalseCtNotNull() throws Exception {
        Path largeDirPath = createLargeDir();
        // setting defaultAcl on root for the first User
        setDefaultAclOnRoot(largeDirPath, this.firstTestUserGuid);
        AbfsConfiguration abfsConfig = getConfiguration(); // update to retrieve fixed test configs
        AzureBlobFileSystem fs = getFileSystem(); // update to retrieve fixed test configs
        AbfsClient client = fs.getAbfsStore().getClient();
        TracingContext testTracingContext = getTestTracingContext(fs, true);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);

        String path = largeDirPath.toString();
        // issuing delete once on large dir to get valid ct
        AbfsRestOperation resultOp = client.deletePath(path, true, null, testTracingContext);
        String continuationToken = getContinuationToken(resultOp.getResult());

        // setting pagination param to false
        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, false);
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                client.deletePath(path, true, continuationToken, getTestTracingContext(fs, false))
        );
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testPaginationFailureWithChangedPath() throws Exception {
        Path largeDirPath = createLargeDir();
        Path smallDirPath = createSmallDir();
        // setting defaultAcl on small directory root for the first User
        setDefaultAclOnRoot(smallDirPath, this.firstTestUserGuid);
        // setting defaultAcl on large directory root for the first User
        setDefaultAclOnRoot(largeDirPath, this.firstTestUserGuid);
        AzureBlobFileSystem fs = getFileSystem();
        AbfsConfiguration abfsConfig = getConfiguration(); // update to retrieve fixed test configs
        AbfsClient client = fs.getAbfsStore().getClient();
        TracingContext testTracingContext = getTestTracingContext(fs, true);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);

        String path1 = "/largeDir";
        // issuing first delete on large directory to obtain
        // non-null continuation token
        AbfsRestOperation resultOp = client.deletePath(path1, true, null, testTracingContext);
        String continuationToken = getContinuationToken(resultOp.getResult());

        // issuing second delete on a different path
        // should fail due to mismatch of continuation token and current delete path
        String path2 = "/smallDir";
        AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
                client.deletePath(path2, true, continuationToken, getTestTracingContext(fs, false))
        );
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, e.getStatusCode());
    }

    @Test
    public void testPaginationFailureWithChangedUser() throws Exception {
        setFirstTestUserFsAuth();
    }

    // test correct no. of resources get deleted based on page size
    // total no. of resources/no. of loops in delete = page size set

    private String getContinuationToken(AbfsHttpOperation resultOp) {
        String continuation = resultOp.getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        return continuation;
    }

    @Test
    public void testDefaultBehaviorWithoutPagination() {

    }

    private Path createLargeDir() throws IOException {
        AzureBlobFileSystem fs = getFileSystem();
        String rootPath = "/largeDir";
        String firstFilePath = rootPath + "/placeholderFile";
        fs.create(new Path(firstFilePath));

        for (int i = 1; i <= 10000; i++) {
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

        for (int i = 1; i <= 5; i++) {
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
