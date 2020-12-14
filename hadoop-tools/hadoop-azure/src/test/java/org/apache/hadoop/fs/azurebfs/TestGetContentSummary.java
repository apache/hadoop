package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
  public void testFileContentSummary() throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(
        new Path("/"+directories[2] + "/test0"));
    Assertions.assertThat(contentSummary.getFileCount())
        .isEqualTo(1);
  }


  @Test
  public void testFileFolderCount()
      throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(new Path("/"));
    System.out.println(contentSummary.toString());
    Assertions.assertThat(contentSummary.getDirectoryCount())
        .describedAs("Directory count does not match")
        .isEqualTo(directories.length);
    Assertions.assertThat(contentSummary.getFileCount())
        .describedAs("File count incorrect")
        .isEqualTo(directories.length * filesPerDirectory);
  }

  @Test
  public void testEmptyDir() throws IOException {
    Path pathToEmptyDir = new Path("/emptyDir");
    fs.mkdirs(pathToEmptyDir);
    ContentSummary contentSummary =
        fs.getContentSummary(pathToEmptyDir);
    Assertions.assertThat(contentSummary.getFileCount())
        .isEqualTo(0);
    Assertions.assertThat(contentSummary.getDirectoryCount())
        .isEqualTo(0);
  }

  void createDirectoryStructure()
      throws IOException, ExecutionException, InterruptedException {
//    AzureBlobFileSystem fs = getFileSystem();
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
