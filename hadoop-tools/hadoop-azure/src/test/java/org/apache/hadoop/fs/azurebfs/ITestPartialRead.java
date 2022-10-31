package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialRead extends AbstractAbfsIntegrationTest {

  private static final String TEST_PATH = "/testfile";

  public ITestPartialRead() throws Exception {
  }

  @Test
  public void testRecoverPartialRead() throws Exception {
    final int fileSize = 4 * ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    final int bufferSize = 4 * ONE_MB;
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally{
      stream.close();
    }
    MockAbfsClient abfsClient = new MockAbfsClient(fs.getAbfsClient());
    MockHttpOperationTestIntercept mockHttpOperationTestIntercept = new MockHttpOperationTestIntercept() {
      @Override
      public void intercept() throws IOException {
        throw new IOException("from the intercept");
      }
    };
    abfsClient.setMockHttpOperationTestIntercept(mockHttpOperationTestIntercept);
    fs.getAbfsStore().setClient(abfsClient);

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);
  }

}
