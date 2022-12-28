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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.AccountSASGenerator;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_FIXED_TOKEN;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAzureBlobFileSystemChooseSAS extends AbstractAbfsIntegrationTest{

    private String accountSAS;

    public ITestAzureBlobFileSystemChooseSAS() throws Exception {
        // The test uses shared key to create a random filesystem and then creates another
        // instance of this filesystem using SAS authorization.
        Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
    }

    private void generateAccountSAS() throws AzureBlobFileSystemException {
        final String accountKey = getConfiguration().getStorageAccountKey();
        AccountSASGenerator configAccountSASGenerator = new AccountSASGenerator(Base64.decode(accountKey));
        accountSAS = configAccountSASGenerator.getAccountSAS(getAccountName());
    }

    @Override
    public void setup() throws Exception {
        createFilesystemForSASTests();
        super.setup();
        // obtaining an account SAS token from in-built generator to set as configuration for testing filesystem level operations
        generateAccountSAS();
    }

    /**
     * Tests the scenario where both the token provider class and a fixed token are configured:
     * whether the correct choice is made (precedence given to token provider class), and the chosen SAS Token works as expected
     * @throws Exception
     */
    @Test
    public void testBothProviderFixedTokenConfigured() throws Exception {
        AbfsConfiguration testAbfsConfig = getConfiguration();

        // configuring a SASTokenProvider class: this provides a user delegation SAS
        // user delegation SAS Provider is set
        // This easily distinguishes between results of filesystem level and blob level operations to ensure correct SAS is chosen,
        // when both a provider class and fixed token is configured.
        testAbfsConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, "org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider");

        // configuring the fixed SAS token
        testAbfsConfig.set(FS_AZURE_SAS_FIXED_TOKEN, accountSAS);

        // creating a new fs instance with the updated configs
        AzureBlobFileSystem newTestFs = (AzureBlobFileSystem) FileSystem.newInstance(testAbfsConfig.getRawConfiguration());

        // testing a file system level operation
        TracingContext tracingContext = getTestTracingContext(newTestFs, true);
        // Expected to fail in the ideal case, as Delegation SAS will be chosen, provider class is given preference when both are configured.
        // This is because filesystem level operations are beyond the scope of a Delegation SAS token.
        intercept(SASTokenProviderException.class,
             () -> {
                    newTestFs.getAbfsStore().getFilesystemProperties(tracingContext);
              });

        // testing blob level operation to ensure delegation SAS token is otherwise valid and above operation fails only because it is fs level
        Path testPath = new Path("/testCorrectSASToken");
        newTestFs.create(testPath).close();
    }

    /**
     * Tests the scenario where only the fixed token is configured, and no token provider class is set:
     * whether fixed token is read correctly from configs, and whether the chosen SAS Token works as expected
     * @throws IOException
     */
    @Test
    public void testOnlyFixedTokenConfigured() throws IOException {
        AbfsConfiguration testAbfsConfig = getConfiguration();

        // clearing any previously configured SAS Token Provider class
        testAbfsConfig.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);

        // setting an account SAS token in the fixed token field
        testAbfsConfig.set(FS_AZURE_SAS_FIXED_TOKEN, accountSAS);

        // creating a new FS with updated configs
        AzureBlobFileSystem newTestFs = (AzureBlobFileSystem) FileSystem.newInstance(testAbfsConfig.getRawConfiguration());

        // attempting an operation using the selected SAS Token
        // as an account SAS is configured, both filesystem level operations (on root) and blob level operations should succeed
        try {
            newTestFs.getFileStatus(new Path("/"));
            Path testPath = new Path("/testCorrectSASToken");
            newTestFs.create(testPath).close();
            newTestFs.delete(new Path("/"), true);
        } catch (Exception e) {
            fail("Exception has been thrown: "+e.getMessage());
        }

    }

    /**
     * Tests the scenario where both the token provider class and the fixed token are not configured:
     * whether the code errors out at the initialization stage itself
     * @throws IOException
     */
    @Test
    public void testBothProviderFixedTokenUnset() throws Exception {
        AbfsConfiguration testAbfsConfig = getConfiguration();

        testAbfsConfig.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
        testAbfsConfig.unset(FS_AZURE_SAS_FIXED_TOKEN);

        intercept(TokenAccessProviderException.class,
                () -> {
                    AzureBlobFileSystem newTestFs = (AzureBlobFileSystem) FileSystem.newInstance(testAbfsConfig.getRawConfiguration());
                });
    }
}
