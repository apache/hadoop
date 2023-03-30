package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class ITestS3AUrlScheme extends AbstractS3ATestBase{

  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return conf;
  }

  @Test
  public void testFSScheme() throws IOException {
    FileSystem fs = getFileSystem();
    assertEquals(fs.getScheme(), "s3a");
    Path path = fs.makeQualified(new Path("tmp/path"));
    assertEquals(path.toUri().getScheme(), "s3a");
  }
}
