package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.EmptyTrashPolicy;
import org.apache.hadoop.fs.Trash;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test Trash for S3AFilesystem.
 */
public class ITestS3ATrash extends AbstractS3ATestBase {

  /**
   * Test default Trash Policy for S3AFilesystem is Empty.
   */
  @Test
  public void testTrashSetToEmptyTrashPolicy() throws IOException {
    Trash trash = new Trash(getFileSystem(), getFileSystem().getConf());
    assertThat(trash.getTrashPolicy()).isInstanceOf(EmptyTrashPolicy.class);
  }
}
