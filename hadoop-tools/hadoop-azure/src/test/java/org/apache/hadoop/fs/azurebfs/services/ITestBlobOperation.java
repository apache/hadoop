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

import java.io.IOException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_BLOB_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestBlobOperation extends AbstractAbfsIntegrationTest {
    private static final int BUFFER_LENGTH = 5;
    private static final int BUFFER_OFFSET = 0;
    private static final String TEST_PATH = "/testfile";
    AzureBlobFileSystem fs;

    public ITestBlobOperation() throws Exception {
        super.setup();
        fs = getFileSystem();
        PrefixMode prefixMode = fs.getAbfsStore().getAbfsConfiguration().getPrefixMode();
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
        AbfsClient abfsClient = fs.getAbfsStore().getClient();

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
        URL url = testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString());

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

    @Test
    public void testParallelCreateBlob() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = fs.getAbfsStore().getClient();

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
        // Updates the query parameters.
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                "abcde", FSOperationType.CREATE,
                TracingHeaderFormat.ALL_ID_FORMAT, null));

        // Create an ExecutorService with 4 threads
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    AbfsRestOperation op = testClient.createPathBlob(testPath.toUri().getPath(),
                            true, false, null, null, null, null, tracingContext);
                    if (op.getResult().getStatusCode() == HTTP_CONFLICT) {
                        throw new IOException("BlobAlreadyExists");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e); // pass the exception through as-is
                }
            }));
        }

        int exceptionCaught = 0;
        for (Future<?> future : futures) {
            try {
                future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
            } catch (ExecutionException e) {
                exceptionCaught++;
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    intercept(RuntimeException.class, () -> {
                        throw (RuntimeException) cause; // re-throw the RuntimeException
                    });
                } else {
                    System.err.println("Unexpected exception caught: " + cause);
                }
            } catch (InterruptedException e) {
                // handle interruption
            }
        }
        assertEquals(exceptionCaught, 4);

        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
    }
}
