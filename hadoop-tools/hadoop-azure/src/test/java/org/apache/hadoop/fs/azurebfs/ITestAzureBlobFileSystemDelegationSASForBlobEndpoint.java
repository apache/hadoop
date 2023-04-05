package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_PENDING;

public class ITestAzureBlobFileSystemDelegationSASForBlobEndpoint extends  ITestAzureBlobFileSystemDelegationSAS {

  public ITestAzureBlobFileSystemDelegationSASForBlobEndpoint()
      throws Exception {
  }

  @Override
  protected void assumptionChecks() throws IOException {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
  }

  @Before
  public void before() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
  }

  @Test
  public void testPosixRenameDirectoryWhereDirectoryAlreadyThereOnDestination() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.mkdirs(new Path("testDir2/test4/test3"));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    Assert.assertFalse(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3")));
    if(getIsNamespaceEnabled(fs) || fs.getAbfsStore().getAbfsConfiguration().get("fs.azure.abfs.account.name").contains(".dfs.core") ||fs.getAbfsStore().getAbfsConfiguration().get("fs.azure.abfs.account.name").contains(".blob.core")) {
      assertFalse(fs.exists(new Path("testDir2/test4/test3/file")));
      assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    } else {
      assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
      assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    }
  }

  @Test
  public void testPosixRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.mkdirs(new Path("testDir2/test4"));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));
  }

  @Test
  public void testCopyBlobTakeTime() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    fileSystem.setAbfsStore(store);
    Mockito.doReturn(COPY_STATUS_PENDING).when(store)
        .getCopyBlobProgress(Mockito.any(AbfsRestOperation.class));
    fileSystem.create(new Path("/test1/file"));
    fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file2")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class), Mockito.any(
            TracingContext.class), Mockito.any(String.class));
  }
}
