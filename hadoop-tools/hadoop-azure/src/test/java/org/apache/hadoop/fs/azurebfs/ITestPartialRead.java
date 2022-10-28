package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialRead extends AbstractAbfsIntegrationTest {

  protected ITestPartialRead() throws Exception {
  }

  @Test
  public void testRecoverPartialRead() throws Exception {
    final int fileSize = 4 * ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsClient abfsClient = Mockito.spy(fs.getAbfsStore().getClient());
    fs.getAbfsStore().setClient(abfsClient);
    Mockito.doAnswer(answer -> {}).when(abfsClient).getA
  }

}
