/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;

public class ITestBlobOperation extends AbstractAbfsIntegrationTest {
    private static final int BUFFER_LENGTH = 5;
    private static final int BUFFER_OFFSET = 0;
    private static final String TEST_PATH = "/testfile";
    AzureBlobFileSystem fs;

    public ITestBlobOperation() throws Exception {
        super.setup();
        fs = getFileSystem();
        PrefixMode prefixMode = getPrefixMode(fs);
        Assume.assumeTrue(prefixMode == PrefixMode.BLOB);
    }

    /**
     * Test helper method to get random bytes array.
     *
     * @param length The length of byte buffer
     * @return byte buffer
     */
    private byte[] getRandomBytesArray(int length) {
        final byte[] b = new byte[length];
        new Random().nextBytes(b);
        return b;
    }

    private String computeMd5(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);

        String md5Base64 = Base64.getEncoder().encodeToString(digest);
        return md5Base64;
    }

    /**
     * Tests the putblob success scenario.
     */
    @Test
    public void testPutBlob() throws Exception {
        // Get the filesystem.
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

        // Gets the client.
        AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));

        byte[] buffer = getRandomBytesArray(5);

        // Create a test container to upload the data.
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));

        // Creates a list of request headers.
        final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
        requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
        requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, BLOCK_BLOB_TYPE));
        String ContentMD5 = computeMd5(buffer);
        // Updates the query parameters.
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();

        // Creates the url for the specified path.
        URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString()));

        // Create a mock of the AbfsRestOperation to set the urlConnection in the corresponding httpOperation.
        AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
                AbfsRestOperationType.PutBlob,
                testClient,
                HTTP_METHOD_PUT,
                url,
                requestHeaders, buffer,
                BUFFER_OFFSET,
                BUFFER_LENGTH, null));

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                "abcde", FSOperationType.CREATE,
                TracingHeaderFormat.ALL_ID_FORMAT, null));

        op.execute(tracingContext);

        // Validate the content by comparing the md5 computed and the value obtained from server
        Assertions.assertThat(op.getResult().getResponseHeader(CONTENT_MD5))
                .describedAs("The content md5 value is not correct")
                .isEqualTo(ContentMD5);
        Assertions.assertThat(op.getResult().getStatusCode())
                .describedAs("The creation failed")
                .isEqualTo(HTTP_CREATED);
    }
}
