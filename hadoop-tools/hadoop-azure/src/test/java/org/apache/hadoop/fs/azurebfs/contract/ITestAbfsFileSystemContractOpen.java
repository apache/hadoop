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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Contract test for open operation.
 */
public class ITestAbfsFileSystemContractOpen extends AbstractContractOpenTest {
  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  public ITestAbfsFileSystemContractOpen() throws Exception {
    binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new AbfsFileSystemContract(conf, isSecure);
  }

  @Override
  public FileSystem getFileSystem() {
    if (!binding.getConfiguration().getHeadOptimizationForInputStream()) {
      return super.getFileSystem();
    }
    try {
      AzureBlobFileSystem fs = (AzureBlobFileSystem) getContract().getTestFileSystem();
      AzureBlobFileSystem spiedFs = Mockito.spy(fs);
      Mockito.doAnswer(answer -> {
        Path path = (Path) answer.getArgument(0);
        FileStatus status = fs.getFileStatus(path);
        if (status.isDirectory()) {
          throw new FileNotFoundException(path.toString());
        }
        return fs.openFile(path)
            .withFileStatus(status)
            .build()
            .join();
      }).when(spiedFs).open(Mockito.any(Path.class));

      Mockito.doAnswer(answer -> {
        Path path = (Path) answer.getArgument(0);
        try {
          FileStatus fileStatus = fs.getFileStatus(path);
          FutureDataInputStreamBuilder builder = Mockito.spy(
              fs.openFile(path).withFileStatus(fileStatus));
          Mockito.doAnswer(builderBuild -> {
            return fs.openFile(path).withFileStatus(fileStatus).build();
          }).when(builder).build();
          return builder;
        } catch (IOException ex) {
          CompletableFuture<FSDataInputStream> future
              = new CompletableFuture<>();
          future.completeExceptionally(ex);

          FutureDataInputStreamBuilder builder = Mockito.spy(fs.openFile(path));
          Mockito.doAnswer(futureAnswer -> {
            futureAnswer.callRealMethod();
            return future;
          }).when(builder).build();
          return builder;
        }
      }).when(spiedFs).openFile(Mockito.any(Path.class));

      Mockito.doNothing().when(spiedFs).close();
      return spiedFs;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
