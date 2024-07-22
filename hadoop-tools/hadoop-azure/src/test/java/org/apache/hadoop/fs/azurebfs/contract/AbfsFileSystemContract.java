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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INPUT_STREAM_LAZY_OPEN_OPTIMIZATION_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_HEAD_OPTIMIZATION_INPUT_STREAM;

/**
 * Azure BlobFileSystem Contract. Test paths are created using any maven fork
 * identifier, if defined. This guarantees paths unique to tests
 * running in parallel.
 */
public class AbfsFileSystemContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "abfs.xml";
  private final boolean isSecure;

  public AbfsFileSystemContract(final Configuration conf, boolean secure) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
    this.isSecure = secure;
  }

  @Override
  public String getScheme() {
    return isSecure ? FileSystemUriSchemes.ABFS_SECURE_SCHEME
            : FileSystemUriSchemes.ABFS_SCHEME;
  }

  @Override
  public Path getTestPath() {
    return new Path(UriUtils.generateUniqueTestPath());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbfsFileSystemContract{");
    sb.append("isSecureScheme=").append(isSecure);
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    final FileSystem fileSystem = super.getTestFileSystem();
    if (!getConf().getBoolean(
        FS_AZURE_INPUT_STREAM_LAZY_OPEN_OPTIMIZATION_ENABLED, DEFAULT_HEAD_OPTIMIZATION_INPUT_STREAM)) {
      return fileSystem;
    }
    try {
      AzureBlobFileSystem fs = (AzureBlobFileSystem) fileSystem;
      AzureBlobFileSystem spiedFs = Mockito.spy(fs);
      Mockito.doAnswer(answer -> {
        Path path = (Path) answer.getArgument(0);
        FileStatus status = fs.getFileStatus(path);

        try {
          return fs.openFile(path)
              .withFileStatus(status)
              .build()
              .join();
        } catch (CompletionException ex) {
          throw ex.getCause();
        }
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
