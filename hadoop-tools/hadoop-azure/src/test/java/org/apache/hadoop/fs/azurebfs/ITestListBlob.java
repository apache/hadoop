/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;


import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ITestListBlob extends
    AbstractAbfsIntegrationTest {

  public ITestListBlob() throws Exception {
    super();
  }

  @Test
  public void testListBlob() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    int i = 0;
    while (i < 10) {
      fs.create(new Path("/dir/" + i));
      i++;
    }
    List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null,
            Mockito.mock(TracingContext.class), null, null, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(11);

    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null,
            Mockito.mock(TracingContext.class), null, null, true);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests")
        .hasSize(10);
  }

  @Test
  public void testListBlobWithMarkers() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    int i = 0;
    while (i < 10) {
      fs.create(new Path("/dir/" + i));
      i++;
    }
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(spiedClient);
    List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null,
            Mockito.mock(TracingContext.class), 1, null, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(11);
    Mockito.verify(spiedClient, Mockito.times(11))
        .getListBlobs(Mockito.nullable(String.class), Mockito.anyString(), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));

    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null,
            Mockito.mock(TracingContext.class), 1, null, true);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(10);
    Mockito.verify(spiedClient, Mockito.times(21))
        .getListBlobs(Mockito.nullable(String.class), Mockito.anyString(), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));
  }

  @Test
  public void testListBlobWithMarkersWithMaxResult() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    int i = 0;
    while (i < 10) {
      fs.create(new Path("/dir/" + i));
      i++;
    }
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(spiedClient);
    List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null,
            Mockito.mock(TracingContext.class), 1, 5, false);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(5);
    Mockito.verify(spiedClient, Mockito.times(5))
        .getListBlobs(Mockito.nullable(String.class), Mockito.anyString(), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));

    blobProperties = fs.getAbfsStore()
        .getListBlobs(new Path("dir"), null,
            Mockito.mock(TracingContext.class), 1, 5, true);
    Assertions.assertThat(blobProperties)
        .describedAs(
            "BlobList should match the number of files created in tests + the directory itself")
        .hasSize(5);
    Mockito.verify(spiedClient, Mockito.times(10))
        .getListBlobs(Mockito.nullable(String.class), Mockito.anyString(), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));
  }

  private void assumeNonHnsAccountBlobEndpoint(final AzureBlobFileSystem fs) {
    Assume.assumeTrue("To work on only on non-HNS Blob endpoint",
        fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
  }
}
