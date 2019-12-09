/*
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

package org.apache.hadoop.yarn.service.provider;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Configuration;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test functionality of ProviderUtils.
 */
public class TestProviderUtils {
  @Test
  public void testStaticFileLocalization() throws IOException {
    // A bunch of mocks ...
    ContainerLaunchService.ComponentLaunchContext compLaunchCtx =
        mock(ContainerLaunchService.ComponentLaunchContext.class);
    AbstractLauncher launcher = mock(AbstractLauncher.class);
    SliderFileSystem sfs = mock(SliderFileSystem.class);
    FileSystem fs = mock(FileSystem.class);
    when(fs.getFileStatus(any(Path.class))).thenAnswer(
        invocationOnMock -> new FileStatus(1L, false, 1, 1L, 1L,
            (Path) invocationOnMock.getArguments()[0]));
    when(fs.exists(any(Path.class))).thenReturn(true);
    when(sfs.getFileSystem()).thenReturn(fs);
    Configuration conf = mock(Configuration.class);
    List<ConfigFile> configFileList = new ArrayList<>();
    when(conf.getFiles()).thenReturn(configFileList);
    when(compLaunchCtx.getConfiguration()).thenReturn(conf);
    when(sfs.createAmResource(any(Path.class), any(LocalResourceType.class)))
        .thenAnswer(invocationOnMock -> new LocalResource() {
          @Override
          public URL getResource() {
            return URL.fromPath(((Path) invocationOnMock.getArguments()[0]));
          }

          @Override
          public void setResource(URL resource) {

          }

          @Override
          public long getSize() {
            return 0;
          }

          @Override
          public void setSize(long size) {

          }

          @Override
          public long getTimestamp() {
            return 0;
          }

          @Override
          public void setTimestamp(long timestamp) {

          }

          @Override
          public LocalResourceType getType() {
            return (LocalResourceType) invocationOnMock.getArguments()[1];
          }

          @Override
          public void setType(LocalResourceType type) {

          }

          @Override
          public LocalResourceVisibility getVisibility() {
            return null;
          }

          @Override
          public void setVisibility(LocalResourceVisibility visibility) {

          }

          @Override
          public String getPattern() {
            return null;
          }

          @Override
          public void setPattern(String pattern) {

          }

          @Override
          public boolean getShouldBeUploadedToSharedCache() {
            return false;
          }

          @Override
          public void setShouldBeUploadedToSharedCache(
              boolean shouldBeUploadedToSharedCache) {

          }
        });

    // Initialize list of files.
    //archive
    configFileList.add(new ConfigFile().srcFile("hdfs://default/sourceFile1")
        .destFile("destFile1").type(ConfigFile.TypeEnum.ARCHIVE));

    //static file
    configFileList.add(new ConfigFile().srcFile("hdfs://default/sourceFile2")
        .destFile("folder/destFile_2").type(ConfigFile.TypeEnum.STATIC));

    //This will be ignored since type is JSON
    configFileList.add(new ConfigFile().srcFile("hdfs://default/sourceFile3")
        .destFile("destFile3").type(ConfigFile.TypeEnum.JSON));
    //No destination file specified
    configFileList.add(new ConfigFile().srcFile("hdfs://default/sourceFile4")
        .type(ConfigFile.TypeEnum.STATIC));

    ProviderUtils.handleStaticFilesForLocalization(launcher, sfs,
        compLaunchCtx);
    Mockito.verify(launcher).addLocalResource(Mockito.eq("destFile1"),
        any(LocalResource.class));
    Mockito.verify(launcher).addLocalResource(
        Mockito.eq("destFile_2"), any(LocalResource.class));
    Mockito.verify(launcher).addLocalResource(
        Mockito.eq("sourceFile4"), any(LocalResource.class));
  }
}
