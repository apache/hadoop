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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.FixedSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Executable;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_FIXED_TOKEN;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

public class ITestAzureBlobFileSystemChooseSAS extends AbstractAbfsIntegrationTest{

    MockSASTokenProvider fixedTokenProvider;
    public ITestAzureBlobFileSystemChooseSAS() throws Exception {
        // These tests rely on specific settings in azure-auth-keys.xml:
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
        fixedTokenProvider = new MockSASTokenProvider();
        fixedTokenProvider.initialize(getRawConfiguration(), getAccountName());
        super.setup();
    }

    @Test
    public void bothProviderConfigSet() throws IOException {
        Configuration testConfig = getRawConfiguration();
        AzureBlobFileSystem testFs = getFileSystem();
        testConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, "org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider");
        // Not setting any operation as Service SAS generator does not use it
        testConfig.set(FS_AZURE_SAS_FIXED_TOKEN, fixedTokenProvider.getSASToken(getAccountName(), testFs.toString(), "/", ""));
        SASTokenProvider actualClass = getConfiguration().getSASTokenProvider();
        // the tokenProvider class should be chosen
        assertEquals(MockDelegationSASTokenProvider.class, actualClass.getClass());

        // attempting an operation using the selected SAS Token
        // creating a new fs instance with the new configs
        AzureBlobFileSystem newTestFs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
        Path testPath = new Path("/testCorrectSASToken");

        newTestFs.create(testPath).close();
    }

    @Test
    public void onlyConfigSet() throws IOException {
        Configuration testConfig = getRawConfiguration();
        AzureBlobFileSystem testFs = getFileSystem();
        testConfig.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
        // Not setting any operation as Service SAS generator does not use it
        testConfig.set(FS_AZURE_SAS_FIXED_TOKEN, fixedTokenProvider.getSASToken(getAccountName(), testFs.toString(), "/", ""));
        SASTokenProvider actualClass = getConfiguration().getSASTokenProvider();
        // the tokenProvider class should be chosen
        assertEquals(FixedSASTokenProvider.class, actualClass.getClass());

        // attempting an operation using the selected SAS Token
        Path testPath = new Path("/testCorrectSASToken");
        testFs.create(testPath).close();
    }

    @Test
    public void onlyProviderSet() throws IOException {
        Configuration testConfig = getRawConfiguration();
        AzureBlobFileSystem testFs = getFileSystem();
        testConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, "org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider");
        testConfig.unset(FS_AZURE_SAS_FIXED_TOKEN);
        SASTokenProvider actualClass = getConfiguration().getSASTokenProvider();
        // the tokenProvider class should be chosen
        assertEquals(MockDelegationSASTokenProvider.class, actualClass.getClass());

        // attempting an operation using the selected SAS Token
        Path testPath = new Path("/testCorrectSASToken");
        testFs.create(testPath).close();
    }

    @Test(expected = TokenAccessProviderException.class)
    public void bothProviderConfigUnset() throws IOException {
        Configuration testConfig = getRawConfiguration();

        testConfig.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
        testConfig.unset(FS_AZURE_SAS_FIXED_TOKEN);

        SASTokenProvider actualClass = getConfiguration().getSASTokenProvider();
    }
}
