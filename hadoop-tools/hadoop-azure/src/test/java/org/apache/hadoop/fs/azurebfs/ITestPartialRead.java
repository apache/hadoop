package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingInterceptTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClientThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialRead extends AbstractAbfsIntegrationTest {

  private static final String TEST_PATH = "/testfile";

  public ITestPartialRead() throws Exception {
  }


  /**
   * Test1: Execute read for 4 MB, but httpOperation will read for only 1MB.:
   * retry with the remaining data, add data in throttlingIntercept.
   * Test2: Execute read for 4 MB, but httpOperation will throw connection-rest exception + read 1 MB:
   * retry with remaining data + add data in throttlingIntercept.
   * */


  private void setup(final Path testPath, final int fileSize) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    final int bufferSize = 4 * ONE_MB;
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);


    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally{
      stream.close();
    }
  }

  @Test
  public void testRecoverPartialRead() throws Exception {

    int fileSize = 4*ONE_MB;
    Path testPath = path(TEST_PATH);
    setup(testPath, fileSize);

    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsClient originalClient = fs.getAbfsClient();
    MockAbfsClient abfsClient = new MockAbfsClient(fs.getAbfsClient());
    MockHttpOperationTestIntercept mockHttpOperationTestIntercept = new MockHttpOperationTestIntercept() {

      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept() throws IOException {
        MockHttpOperationTestInterceptResult mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.status = 206;
        mockHttpOperationTestInterceptResult.bytesRead = ONE_MB;
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }
      public int getCallCount() {
        return callCount;
      }
    };
    abfsClient.setMockHttpOperationTestIntercept(mockHttpOperationTestIntercept);
    fs.getAbfsStore().setClient(abfsClient);

    AbfsClientThrottlingIntercept intercept = AbfsClientThrottlingInterceptTestUtil.get();
    MockAbfsClientThrottlingAnalyzer readAnalyzer = new MockAbfsClientThrottlingAnalyzer("read");
    AbfsClientThrottlingInterceptTestUtil.setReadAnalyzer(intercept, readAnalyzer);

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);

    Assert.assertEquals(4, mockHttpOperationTestIntercept.getCallCount());
    Assert.assertEquals(4, readAnalyzer.getFailedInstances());
  }

}
