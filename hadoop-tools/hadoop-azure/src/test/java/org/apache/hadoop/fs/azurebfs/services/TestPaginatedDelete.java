package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.assertj.core.api.Assertions;

import java.io.IOException;
import java.net.HttpURLConnection;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;

public class TestPaginatedDelete extends AbstractAbfsIntegrationTest {
    public TestPaginatedDelete() throws Exception {
        loadConfiguredFileSystem();
        boolean isHnsEnabled = this.getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
        Assume.assumeTrue(isHnsEnabled);
    }

    @Test
    public void testVersionForPagination() throws Exception {
        createLargeDir();
        AbfsConfiguration abfsConfig = getConfiguration(); // update to retrieve fixed test configs
        AzureBlobFileSystem fs = getFileSystem(); // update to retrieve fixed test configs
        AbfsClient client = fs.getAbfsStore().getClient();
        client = TestAbfsClient.setAbfsClientField(client, "xMsVersion", "2021-12-02");

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, true);
        updateTestConfiguration();

        // delete should fail with bad request as version does not support pagination
        String path = "/LargeDir";
        AbfsRestOperation resultOp = client.deletePath(path, true, null, getTestTracingContext(fs, false));
        int statusCode = resultOp.getResult().getStatusCode();
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, statusCode);

        abfsConfig.setBoolean(ConfigurationKeys.FS_AZURE_ENABLE_PAGINATED_DELETE, false);
        updateTestConfiguration();

        resultOp = client.deletePath(path, true, null, getTestTracingContext(fs, false));
        statusCode = resultOp.getResult().getStatusCode();
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, statusCode);
    }


    @Test
    public void testDefaultBehaviorWithoutPagination() {

    }

    @Before
    public void createLargeDir() throws IOException {
        AzureBlobFileSystem fs = getFileSystem();
        String rootPath = "/largeDir";
        String firstFilePath = rootPath + "/placeholderFile";
        fs.create(new Path(firstFilePath));

        for (int i = 1; i <= 2000; i++) {
            String dirPath = "/dir" + String.valueOf(i);
            String filePath = rootPath + dirPath + "/file" + String.valueOf(i);
            fs.create(new Path(filePath));
        }

    }

    private void fixTestConfiguration() {

    }

    private void updateTestConfiguration() {

    }

    private String getSmallDirPath() {
        return "abc";
    }

    private String getLargeDirPath() {
        return "abc";
    }

}
