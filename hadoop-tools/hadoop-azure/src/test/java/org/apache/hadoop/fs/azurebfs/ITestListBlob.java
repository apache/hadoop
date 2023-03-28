package org.apache.hadoop.fs.azurebfs;


import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ITestListBlob extends
    AbstractAbfsIntegrationTest {

  public ITestListBlob() throws Exception {
    super();
  }

  @Test
  public void testListBlob() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    if (fs.getAbfsStore().getAbfsConfiguration().getIsNamespaceEnabledAccount()
        == Trilean.TRUE) {
      return;
    }
    int i = 0;
    while (i < 10) {
      fs.create(new Path("/dir/" + i));
      i++;
    }
    List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"),
            Mockito.mock(TracingContext.class), null);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(11);
  }

  @Test
  public void testListBlobWithMarkers() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    if (fs.getAbfsStore().getAbfsConfiguration().getIsNamespaceEnabledAccount()
        == Trilean.TRUE) {
      return;
    }
    int i = 0;
    while (i < 10) {
      fs.create(new Path("/dir/" + i));
      i++;
    }
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(spiedClient);
    List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"),
            Mockito.mock(TracingContext.class), 1);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(11);
    Mockito.verify(spiedClient, Mockito.times(11))
        .getListBlobs(Mockito.any(Path.class),
            Mockito.any(TracingContext.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.anyInt());
  }
}
