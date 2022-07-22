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

import org.junit.Test;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.mockito.Mockito;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.AzureBlobFileSystemStoreBuilder;
import java.nio.file.AccessDeniedException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class TestAzureBlobFileSystem extends AbstractAbfsIntegrationTest{

  public TestAzureBlobFileSystem() throws Exception {
  }

  @Test
  public void testCreateFileSystem() throws Exception {
    AzureBlobFileSystem fs = new AzureBlobFileSystem();
    Configuration conf = this.getRawConfiguration();
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    fs.initialize(FileSystem.getDefaultUri(conf),conf);
    Assert.assertNotNull(fs.getFileStatus(new Path(AbfsHttpConstants.ROOT_PATH)));
    AzureBlobFileSystemStore abfsStore = Mockito.mock(AzureBlobFileSystemStore.class);
    AzureBlobFileSystem azureBlobFileSystem = Mockito.spy(AzureBlobFileSystem.class);
//    doThrow(new AccessDeniedException("Not permissible")).when(abfsStore).
//        getFileStatus(any(Path.class), any(TracingContext.class));
//    TestAzureBlobFileSystem.setAbfsStore(abfsStore, azureBlobFileSystem);
    doReturn(abfsStore).when(azureBlobFileSystem).getAzureBlobFileSystemStore(any(AzureBlobFileSystemStoreBuilder.class));
    doReturn(this.getConfiguration()).when(abfsStore).getAbfsConfiguration();
    doThrow(new AccessDeniedException("Not permissible")).when(abfsStore).
        getFileStatus(any(Path.class), any(TracingContext.class));
    azureBlobFileSystem.initialize(FileSystem.getDefaultUri(conf),conf);
  }

  public static void setAbfsStore(AzureBlobFileSystemStore abfsStore, AzureBlobFileSystem fs) {
    Mockito.doReturn(abfsStore).when(fs).getAbfsStore();
  }
}
