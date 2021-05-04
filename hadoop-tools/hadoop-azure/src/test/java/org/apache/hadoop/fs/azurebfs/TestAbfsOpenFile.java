package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.impl.OpenFileParameters;

public class TestAbfsOpenFile extends AbstractAbfsIntegrationTest {

  public TestAbfsOpenFile() throws Exception {
  }

  @Test
  public void testOpen() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testFile");
    fs.create(path);
    fs.open(path);
  }

  @Test
  public void testOpenWithEmptyOpenFileParams()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testFile");
    fs.create(path);
    fs.openFileWithOptions(path,
        new OpenFileParameters()).get();
  }

  @Test
  public void testOpenWithValidFileStatus()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testFile");
    fs.create(path);
    FileStatus fileStatus = fs.getFileStatus(path);
    FSDataOutputStream out = fs.append(path);
    out.write(1);
    out.close();

    // should not invoke GetPathStatus
    FSDataInputStream in = fs.openFileWithOptions(path,
        new OpenFileParameters().withStatus(fileStatus)).get();

    assertEquals("One byte should be written to file", 1,
        fs.getFileStatus(path).getLen());
    assertEquals(
        "InputStream was created with old fileStatus, so contentLength "
            + "reflected should be 0 post write", 0,
        ((AbfsInputStream) in.getWrappedStream()).length());
  }

  @Test
  public void testOpenWithOptions()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testFile");
    fs.create(path);
    Configuration testOptions = new Configuration();
    fs.openFileWithOptions(path,
        new OpenFileParameters().withOptions(testOptions)).get();
  }

  @Test
  public void testNonVersionedFileStatus() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testFile");
    fs.create(path);
    FileStatus fileStatus = new FileStatus(); // no version specified
    //Request with FileStatus without eTag should succeed using GetPathStatus
    fs.openFileWithOptions(path,
        new OpenFileParameters().withStatus(fileStatus));
  }

}
