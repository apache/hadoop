package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.P;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestGetContentSummary extends AbstractAbfsIntegrationTest {
  private final String[] directories = {"testFolder", "testFolder/testFolder1",
      "testFolder/testFolder2", "testFolder/testFolder3", "testFolderII",
      "testFolder/testFolder2/testFolder4", "testFolder/testFolder2/testFolder5",
      "testFolder/testFolder3/testFolder6", "testFolder/testFolder3/testFolder7"};
  private final int filesPerDirectory = 2;
  private final AzureBlobFileSystem fs = createFileSystem();

  public TestGetContentSummary() throws Exception {
    createDirectoryStructure();
  }

  @Test
  public void testFilesystemRoot()
      throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(new Path("/"));
    System.out.println(contentSummary.toString());
    checkContentSummary(contentSummary, directories.length,
        directories.length * filesPerDirectory, 0);
  }

  @Test
  public void testFileContentSummary() throws IOException {
    Path filePath = new Path("/testFolderII/testFile");
    FSDataOutputStream out = fs.create(filePath);
    byte[] b = new byte[20];
    new Random().nextBytes(b);
    out.write(b);
    out.hsync();
    ContentSummary contentSummary = fs.getContentSummary(filePath);
    checkContentSummary(contentSummary, 0, 1, 20);
  }

  @Test
  public void testLeafDir() throws IOException {
    Path pathToLeafDir =
        new Path("/testFolder/testFolder2/testFolder4"
        + "/leafDir");
    fs.mkdirs(pathToLeafDir);
    ContentSummary contentSummary = fs.getContentSummary(pathToLeafDir);
    checkContentSummary(contentSummary, 0, 0, 0);
  }

  @Test
  public void testIntermediateDirWithFilesOnly() throws IOException {
    String dirPath = "/testFolder/testFolder3/testFolder6";
    byte[] b = new byte[20];
    new Random().nextBytes(b);
    for (int i = 0; i < filesPerDirectory; i++) {
      FSDataOutputStream out = fs.append(new Path(dirPath + "/test" + i));
      out.write(b);
      out.close();
    }
    ContentSummary contentSummary = fs.getContentSummary(new Path(dirPath));
    checkContentSummary(contentSummary, 0, filesPerDirectory, 20 * filesPerDirectory);
  }

  @Test
  public void testIntermediateDirWithFilesAndSubdirs() throws IOException {
    Path dirPath = new Path("/testFolder/testFolder3");
    byte[] b = new byte[20];
    new Random().nextBytes(b);
    for (int i = 0; i < filesPerDirectory; i++) {
      FSDataOutputStream out = fs.append(new Path(dirPath + "/test" + i));
      out.write(b);
      out.close();
    }
    Path dir2Path = new Path("/testFolder/testFolder3/testFolder6");
    for (int i = 0; i < filesPerDirectory; i++) {
      FSDataOutputStream out = fs.append(new Path(dir2Path + "/test" + i));
      out.write(b);
      out.close();
    }
    ContentSummary contentSummary = fs.getContentSummary(dirPath);
    checkContentSummary(contentSummary, 2, 3 * filesPerDirectory, 20 * 2 * 2);
  }

  @Test
  public void testEmptyDir() throws IOException {
    Path pathToEmptyDir = new Path("/testFolder/emptyDir");
    fs.mkdirs(pathToEmptyDir);
    ContentSummary contentSummary =
        fs.getContentSummary(pathToEmptyDir);
    checkContentSummary(contentSummary, 0, 0, 0);
  }

  private void checkContentSummary(ContentSummary contentSummary,
      long directoryCount, long fileCount, long byteCount) {
    Assertions.assertThat(contentSummary.getDirectoryCount())
        .describedAs("Incorrect directory count")
        .isEqualTo(directoryCount);
    Assertions.assertThat(contentSummary.getFileCount())
        .describedAs("Incorrect file count")
        .isEqualTo(fileCount);
    Assertions.assertThat(contentSummary.getLength())
        .describedAs("Incorrect length")
        .isEqualTo(byteCount);
    Assertions.assertThat(contentSummary.getSpaceConsumed())
        .describedAs("Incorrect value of space consumed")
        .isEqualTo(byteCount);
  }

  private void createDirectoryStructure()
      throws IOException, ExecutionException, InterruptedException {
    for (String directory : directories) {
      fs.mkdirs(new Path("/" + directory));
    }
    for (String directory : directories) {
      final List<Future<Void>> tasks = new ArrayList<>();
      ExecutorService es = Executors.newFixedThreadPool(10);
      for (int i = 0; i < filesPerDirectory; i++) {
        final Path fileName = new Path("/" + directory + "/test" + i);
        tasks.add(es.submit(() -> {
          touch(fileName);
          return null;
        }));
      }
      for (Future<Void> task : tasks) {
        task.get();
      }
      es.shutdownNow();
    }
  }
}
