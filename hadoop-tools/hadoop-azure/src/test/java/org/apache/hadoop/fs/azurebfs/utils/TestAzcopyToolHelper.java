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

package org.apache.hadoop.fs.azurebfs.utils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

public class TestAzcopyToolHelper extends AbstractAbfsIntegrationTest {

  public TestAzcopyToolHelper() throws Exception {

  }

  @Test
  public void testGetAzcopyToolCommand() throws Exception {
    AzureBlobFileSystem fs = this.getFileSystem();
    Path filePath = path("dir/file.txt");
    Path implicitDirPath = path("dir1");
    Path explicitDirPath = path("dir2/dir3");
    Path nonExistentPath = path("dir/nonexistent");
    this.createAzCopyFile(filePath);
    this.createAzCopyFolder(implicitDirPath);
    fs.mkdirs(explicitDirPath);

    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(
            filePath.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("File created by azcopy should have implicit parent")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(
            implicitDirPath, fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(
            explicitDirPath, fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(
            explicitDirPath.getParent(), fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(
            filePath, fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isFalse();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(
            filePath, fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isFalse();
    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(
            nonExistentPath, fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isFalse();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(
            nonExistentPath, fs, getTestTracingContext(fs, false)))
        .describedAs("Folder created by azcopy should be implicit")
        .isFalse();
  }
}
