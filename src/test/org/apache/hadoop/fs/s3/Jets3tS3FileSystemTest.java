package org.apache.hadoop.fs.s3;

import java.io.IOException;

public class Jets3tS3FileSystemTest extends S3FileSystemBaseTest {

  @Override
  public FileSystemStore getFileSystemStore() throws IOException {
    return null; // use default store
  }

}
