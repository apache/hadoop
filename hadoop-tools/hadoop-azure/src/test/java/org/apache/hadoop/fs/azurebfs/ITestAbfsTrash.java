package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.EmptyTrashPolicy;
import org.apache.hadoop.fs.Trash;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests to verify behaviour of Trash with AzureBlobFileSystem.
 */
public class ITestAbfsTrash extends AbstractAbfsIntegrationTest {

  public ITestAbfsTrash() throws Exception {}

  /**
   * Test default Trash Policy for S3AFilesystem is Empty.
   */
  @Test
  public void testTrashSetToEmptyTrashPolicy() throws IOException {
    Trash trash = new Trash(getFileSystem(), getFileSystem().getConf());
    assertThat(trash.getTrashPolicy()).isInstanceOf(EmptyTrashPolicy.class);

  }
}
