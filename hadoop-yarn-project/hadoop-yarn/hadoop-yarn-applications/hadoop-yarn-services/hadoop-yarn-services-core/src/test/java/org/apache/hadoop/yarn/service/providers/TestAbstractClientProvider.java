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
package org.apache.hadoop.yarn.service.providers;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.provider.AbstractClientProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the AbstractClientProvider shared methods.
 */
public class TestAbstractClientProvider {
  private static final String EXCEPTION_PREFIX = "Should have thrown " +
      "exception: ";
  private static final String NO_EXCEPTION_PREFIX = "Should not have thrown " +
      "exception: ";

  private static class ClientProvider extends AbstractClientProvider {
    @Override
    public void validateArtifact(Artifact artifact, String compName,
        FileSystem fileSystem) throws IOException {
    }

    @Override
    protected void validateConfigFile(ConfigFile configFile, String compName,
        FileSystem fileSystem) throws IOException {
    }
  }

  @Test
  public void testConfigFiles() throws IOException {
    ClientProvider clientProvider = new ClientProvider();
    FileSystem mockFs = mock(FileSystem.class);
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFs.exists(any())).thenReturn(true);

    String compName = "sleeper";
    ConfigFile configFile = new ConfigFile();
    List<ConfigFile> configFiles = new ArrayList<>();
    configFiles.add(configFile);

    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "null file type");
    } catch (IllegalArgumentException e) {
    }

    configFile.setType(ConfigFile.TypeEnum.TEMPLATE);
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "empty src_file for type template");
    } catch (IllegalArgumentException e) {
    }

    configFile.setSrcFile("srcfile");
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "empty dest file");
    } catch (IllegalArgumentException e) {
    }

    configFile.setDestFile("destfile");
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    configFile = new ConfigFile();
    configFile.setType(ConfigFile.TypeEnum.JSON);
    configFile.setSrcFile(null);
    configFile.setDestFile("path/destfile2");
    configFiles.add(configFile);
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "dest file with multiple path elements");
    } catch (IllegalArgumentException e) {
    }

    configFile.setDestFile("/path/destfile2");
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getMessage());
    }

    configFile.setDestFile("destfile");
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "duplicate dest file");
    } catch (IllegalArgumentException e) {
    }

    configFiles.clear();
    configFile = new ConfigFile();
    configFile.setType(ConfigFile.TypeEnum.STATIC);
    configFile.setSrcFile(null);
    configFile.setDestFile("path/destfile3");
    configFiles.add(configFile);
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "dest file with multiple path elements");
    } catch (IllegalArgumentException e) {
    }

    configFile.setDestFile("/path/destfile3");
    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "src file should be specified");
    } catch (IllegalArgumentException e) {
    }

    //should succeed
    configFile.setSrcFile("srcFile");
    configFile.setDestFile("destfile3");
    clientProvider.validateConfigFiles(configFiles, compName, mockFs);

    when(mockFileStatus.isDirectory()).thenReturn(true);
    when(mockFs.getFileStatus(new Path("srcFile")))
        .thenReturn(mockFileStatus).thenReturn(mockFileStatus);

    configFiles.clear();
    configFile = new ConfigFile();
    configFile.setType(ConfigFile.TypeEnum.STATIC);
    configFile.setSrcFile("srcFile");
    configFile.setDestFile("destfile3");
    configFiles.add(configFile);

    try {
      clientProvider.validateConfigFiles(configFiles, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + "src file is a directory");
    } catch (IllegalArgumentException e) {
    }
  }
}
