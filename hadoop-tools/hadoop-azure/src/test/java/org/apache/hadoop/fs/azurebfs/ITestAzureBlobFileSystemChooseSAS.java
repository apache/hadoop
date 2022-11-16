package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.Assume;
import org.junit.Test;

public class ITestAzureBlobFileSystemChooseSAS extends AbstractAbfsIntegrationTest {

    protected ITestAzureBlobFileSystemChooseSAS() throws Exception {
        Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_ID));
        Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SECRET));
        Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID));
        Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID));
        // The test uses shared key to create a random filesystem and then creates another
        // instance of this filesystem using SAS authorization.
        Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
    }

    @Override
    public void setup() throws Exception {
        createFilesystemForSASTests();
    }

    @Test
    public void setBothProviderFixedToken() {
        AbfsConfiguration testConfig = this.getConfiguration();
        testConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER)

    }
}
