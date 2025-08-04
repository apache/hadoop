package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_BLOCK_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Test class for ReadBufferManagerV2 functionality.
 */
public class ITestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  private static final String TEST_FILE_NAME_PREFIX = "testFile";
  private static final int LESS_NUM_FILES = 5;
  private static final int MORE_NUM_FILES = 10;
  private static final int SMALL_FILE_SIZE = 30 * ONE_MB;
  private static final int LARGE_FILE_SIZE = 100 * ONE_MB;

  public ITestReadBufferManagerV2() throws Exception {
    super();
    getConfiguration().setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
  }

  /**
   * Test to verify that ReadBufferManagerV2 can read different files concurrently.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadBufferManagerV2() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(FS_AZURE_READ_AHEAD_BLOCK_SIZE, String.valueOf(4 * ONE_MB));
    try (AzureBlobFileSystem fs = getConfiguredFileSystem(true, configMap)) {
      int numOfFiles = LESS_NUM_FILES;
      Path[] testPaths = createFilesWithContent(fs, numOfFiles);
      ExecutorService executorService = Executors.newFixedThreadPool(
          numOfFiles);

      int[] fileIdx = new int[1];
      try {
        for (int i = 0; i < numOfFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = fs.open(testPaths[fileIdx[0]++])) {
              int bytesRead = iStream.read(new byte[SMALL_FILE_SIZE], 0,
                  SMALL_FILE_SIZE);
              Assertions.assertEquals(SMALL_FILE_SIZE, bytesRead,
                  "Read size should match file size");
            }
            return null;
          });
        }
      } catch (Exception e) {
        System.out.println(
            "Exception occurred during file read: " + e.getMessage());
      } finally {
        shutdownExecutorService(executorService);
      }
    }
  }

  /**
   * Test to verify that multiple input streams can read the same file.
   * With read ahead v2 enabled, multiple input stream read the same cached buffer
   * based on file eTag.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testMultipleInputStreamReadingSameFile() throws Exception {
    AzureBlobFileSystem spiedFs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    int numOfFiles = MORE_NUM_FILES;
    int fileSize = SMALL_FILE_SIZE;
    int blockSize = 4 * ONE_MB;

    Path[] testPaths = createFilesWithContent(spiedFs, 1);
    Path testPath = testPaths[0];
    ExecutorService executorService = Executors.newFixedThreadPool(
        LESS_NUM_FILES);

    try {
      for (int i = 0; i < LESS_NUM_FILES; i++) {
        executorService.submit((Callable<Void>) () -> {
          try (FSDataInputStream iStream = spiedFs.open(testPath)) {
            int bytesRead = iStream.read(new byte[LARGE_FILE_SIZE], 0,
                LARGE_FILE_SIZE);
            Assertions.assertEquals(LARGE_FILE_SIZE, bytesRead,
                "Read size should match file size");
          }
          return null;
        });
      }
    } finally {
      shutdownExecutorService(executorService);
    }

    int leastReadOpnCount = fileSize/blockSize;

    verify(spiedClient, Mockito.atLeast(leastReadOpnCount))
        .read(Mockito.anyString(), Mockito.anyLong(), Mockito.any(),
            Mockito.anyInt(), Mockito.anyInt(), Mockito.anyString(),
            Mockito.anyString(), Mockito.any(), Mockito.any());
    verify(spiedClient, Mockito.atMost(2 * leastReadOpnCount))
        .read(eq(testPath.toString()), Mockito.anyLong(), Mockito.any(),
            Mockito.anyInt(), Mockito.anyInt(), Mockito.anyString(),
            Mockito.anyString(), Mockito.any(), Mockito.any());

  }

  /**
   * Test to verify that scheduled eviction of completed buffers happens.
   * This test will be implemented in the future.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testScheduledEviction() throws Exception {
  }

  private AzureBlobFileSystem getConfiguredFileSystem(boolean isRAV2Enabled,
      Map<String, String> configurations) throws IOException {
    Configuration conf = getConfiguration().getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, isRAV2Enabled);
    for (Map.Entry<String, String> entry : configurations.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws
      IOException {
    Path testFilePath = path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  private Path[] createFilesWithContent(FileSystem fs,
      int numFiles) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numFiles);
    Path[] tesFilePaths = new Path[numFiles];
    int[] fileIdx = new int[1];
    try {
      for (int i = 0; i < numFiles; i++) {
        final String fileName = ITestReadBufferManagerV2.TEST_FILE_NAME_PREFIX + i;
        executorService.submit((Callable<Void>) () -> {
          byte[] fileContent = getRandomBytesArray(
              ITestReadBufferManagerV2.SMALL_FILE_SIZE);
          tesFilePaths[fileIdx[0]++] = createFileWithContent(fs, fileName, fileContent);
          return null;
        });
      }
    } finally {
      shutdownExecutorService(executorService);
    }
    return tesFilePaths;
  }

  private void shutdownExecutorService(ExecutorService executorService) {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }
  }
}
