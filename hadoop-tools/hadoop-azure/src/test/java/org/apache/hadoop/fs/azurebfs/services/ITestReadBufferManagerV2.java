package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  private static final String TEST_FILE_NAME_PREFIX = "testFile";
  private static final int LESS_NUM_FILES = 5;
  private static final int MORE_NUM_FILES = 10;
  private static final int SMALL_FILE_SIZE = 30 * ONE_MB;
  private static final int LARGE_FILE_SIZE = 200 * ONE_MB;

  public ITestReadBufferManagerV2() throws Exception {
    super();
    getConfiguration().set(FS_AZURE_ENABLE_READAHEAD_V2, "true");
  }

  @Test
  public void testReadBufferManagerV2() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path[] testPaths = createFilesWithContent(fs, TEST_FILE_NAME_PREFIX,
        LESS_NUM_FILES, SMALL_FILE_SIZE);
    ExecutorService executorService = Executors.newFixedThreadPool(LESS_NUM_FILES);

    int[] fileIdx = new int[1];
    try {
      for (int i = 0; i < LESS_NUM_FILES; i++) {
        executorService.submit((Callable<Void>) () -> {
          try (FSDataInputStream iStream = fs.open(testPaths[fileIdx[0]++])) {
            int bytesRead = iStream.read(new byte[SMALL_FILE_SIZE], 0, SMALL_FILE_SIZE);
            Assertions.assertEquals(SMALL_FILE_SIZE, bytesRead,
                "Read size should match file size");
          }
          return null;
        });
      }
    } catch(Exception e) {
      System.out.println("Exception occurred during file read: " + e.getMessage());
    } finally {
      executorService.shutdown();
      // wait for all tasks to finish
      executorService.awaitTermination(1, TimeUnit.MINUTES);
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

    int fileSize = SMALL_FILE_SIZE;
    int numOfFile = LESS_NUM_FILES;
    Path[] testPaths = createFilesWithContent(spiedFs, TEST_FILE_NAME_PREFIX,
        1, fileSize);
    Path testPath = testPaths[0];
    ExecutorService executorService = Executors.newFixedThreadPool(numOfFile);

    try {
      for (int i = 0; i < LESS_NUM_FILES; i++) {
        executorService.submit((Callable<Void>) () -> {
          try (FSDataInputStream iStream = spiedFs.open(testPath)) {
            int bytesRead = iStream.read(new byte[LARGE_FILE_SIZE], 0, LARGE_FILE_SIZE);
            Assertions.assertEquals(LARGE_FILE_SIZE, bytesRead,
                "Read size should match file size");
          }
          return null;
        });
      }
    } catch(Exception e) {
      System.out.println("Exception occurred during file read: " + e.getMessage());
    } finally {
      executorService.shutdown();
      // wait for all tasks to finish
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
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
      String fileNamePrefix,
      int numFiles,
      int fileSize) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numFiles);
    Path[] tesFilePaths = new Path[numFiles];
    int[] fileIdx = new int[1];
    try {
      for (int i = 0; i < numFiles; i++) {
        final String fileName = fileNamePrefix + i;
        executorService.submit((Callable<Void>) () -> {
          byte[] fileContent = getRandomBytesArray(fileSize);
          tesFilePaths[fileIdx[0]++] = createFileWithContent(fs, fileName, fileContent);
          return null;
        });
      }
    } finally {
      executorService.shutdown();
      // wait for all tasks to finish
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
    return tesFilePaths;
  }
}
