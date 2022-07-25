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
import org.mockito.Mockito;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.mockito.ArgumentMatchers.any;
import org.apache.hadoop.test.LambdaTestUtils;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.AzureBlobFileSystemStoreBuilder;
import java.nio.file.AccessDeniedException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class TestAzureBlobFileSystem extends AbstractAbfsIntegrationTest {
  public TestAzureBlobFileSystem() throws Exception {
  }

  @Test
  public void testCreateFileSystem() throws Exception {
    AzureBlobFileSystem fs = new AzureBlobFileSystem();
    Configuration conf = this.getRawConfiguration();
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);

    //Create a file system
    fs.initialize(FileSystem.getDefaultUri(conf), conf);

    //Since fileSystem is created, getFileStatus should not return null
    Assert.assertNotNull(
        fs.getFileStatus(new Path(AbfsHttpConstants.ROOT_PATH)));

    //Throw exception for getFileStatus and see if it gets caught correctly
    AzureBlobFileSystemStore abfsStore = Mockito.mock(
        AzureBlobFileSystemStore.class);
    AzureBlobFileSystem azureBlobFileSystem = Mockito.spy(getFileSystem());
    doReturn(abfsStore).when(azureBlobFileSystem)
        .getAzureBlobFileSystemStore(
            any(AzureBlobFileSystemStoreBuilder.class));
    AbfsConfiguration abfsConfiguration = Mockito.mock(AbfsConfiguration.class);
    Mockito.doReturn(true)
        .when(abfsConfiguration)
        .getCreateRemoteFileSystemDuringInitialization();
    Mockito.doReturn(this.getConfiguration().getClientCorrelationId())
        .when(abfsConfiguration)
        .getClientCorrelationId();
    Mockito.doReturn(abfsConfiguration).when(abfsStore).getAbfsConfiguration();
    doThrow(new AccessDeniedException("Not permissible")).when(abfsStore).
        getFileStatus(any(Path.class), any(TracingContext.class));
    LambdaTestUtils.intercept(AccessDeniedException.class, () -> {
      azureBlobFileSystem.initialize(FileSystem.getDefaultUri(conf), conf);
    });
  }
}
