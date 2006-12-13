package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public class TestInMemoryS3FileSystem extends S3FileSystemBaseTest {

  @Override
  public FileSystemStore getFileSystemStore() throws IOException {
    return new InMemoryFileSystemStore();
  }
  
  public void testInitialization() throws IOException {
    initializationTest("s3://a:b@c", "s3://a:b@c");
    initializationTest("s3://a:b@c/", "s3://a:b@c");
    initializationTest("s3://a:b@c/path", "s3://a:b@c");
    initializationTest("s3://a@c", "s3://a@c");
    initializationTest("s3://a@c/", "s3://a@c");
    initializationTest("s3://a@c/path", "s3://a@c");
    initializationTest("s3://c", "s3://c");
    initializationTest("s3://c/", "s3://c");
    initializationTest("s3://c/path", "s3://c");
  }
  
  private void initializationTest(String initializationUri, String expectedUri) throws IOException {
    S3FileSystem fs = new S3FileSystem(getFileSystemStore());
    fs.initialize(URI.create(initializationUri), new Configuration());
    assertEquals(URI.create(expectedUri), fs.getUri());
  }

}
