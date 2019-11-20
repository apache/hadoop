/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.executor;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderFactory;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class MetadataSyncOperationExecutor {

  public static final Logger LOG =
      LoggerFactory.getLogger(MetadataSyncOperationExecutor.class);

  private Configuration conf;

  private MetadataSyncOperationExecutor(Configuration conf) {
    this.conf = conf;
  }

  public static MetadataSyncOperationExecutor createOnNameNode(
      Configuration conf) {
    return new MetadataSyncOperationExecutor(conf);
  }

  public SyncTaskExecutionResult execute(MetadataSyncTask metadataSyncTask) throws Exception {
    LOG.info("Executing MetadataSyncTask {} ({} on {})",
        metadataSyncTask.getSyncTaskId(), metadataSyncTask.getOperation(), metadataSyncTask.getUri());

    switch (metadataSyncTask.getOperation()) {
    case MULTIPART_INIT:
      MetadataSyncTask.MultipartInitMetadataSyncTask cfSyncTask =
          (MetadataSyncTask.MultipartInitMetadataSyncTask) metadataSyncTask;
      return doMultiPartInit(cfSyncTask.getUri());
    case MULTIPART_COMPLETE:
      MetadataSyncTask.MultipartCompleteMetadataSyncTask mpcSyncTask =
          (MetadataSyncTask.MultipartCompleteMetadataSyncTask) metadataSyncTask;
      return doMultiPartComplete(mpcSyncTask.getUri(),
          mpcSyncTask.blocks, mpcSyncTask.uploadHandle,
          mpcSyncTask.partHandles);
    case TOUCH_FILE:
      return doTouchFile(metadataSyncTask.getUri());
    case CREATE_DIRECTORY:
      MetadataSyncTask.CreateDirectoryMetadataSyncTask cdSyncTask =
          (MetadataSyncTask.CreateDirectoryMetadataSyncTask) metadataSyncTask;
      return doCreateDirectory(cdSyncTask.getUri());
    case DELETE_FILE:
      MetadataSyncTask.DeleteFileMetadataSyncTask dfSyncTask =
          (MetadataSyncTask.DeleteFileMetadataSyncTask) metadataSyncTask;
      return doDeleteFile(dfSyncTask.getUri(), dfSyncTask.getBlocks());
    case DELETE_DIRECTORY:
      MetadataSyncTask.DeleteDirectoryMetadataSyncTask ddSyncTask =
          (MetadataSyncTask.DeleteDirectoryMetadataSyncTask) metadataSyncTask;
      doDeleteDirectory(ddSyncTask.getUri());
      return SyncTaskExecutionResult.emptyResult();
    case RENAME_FILE:
      MetadataSyncTask.RenameFileMetadataSyncTask rfSyncTask =
          (MetadataSyncTask.RenameFileMetadataSyncTask) metadataSyncTask;
      return doRename(rfSyncTask.getUri(), rfSyncTask.renamedTo);
    case RENAME_DIRECTORY:
      MetadataSyncTask.RenameDirectoryMetadataSyncTask rdSyncTask =
          (MetadataSyncTask.RenameDirectoryMetadataSyncTask) metadataSyncTask;
      return doRenameDirectory(rdSyncTask.getUri(), rdSyncTask.renamedTo);
    default:
      LOG.warn("MetadataSyncTask {} can not be executed: operation {} not implemented in {}",
          metadataSyncTask.getSyncTaskId(), metadataSyncTask.getOperation(),
          MetadataSyncOperationExecutor.class);
      throw new UnsupportedOperationException("Operation " + metadataSyncTask
          .getOperation() + " not implemented");
    }
  }

  private SyncTaskExecutionResult doTouchFile(URI uri) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path filePath = new Path(uri);
    fs.create(filePath).close();
    return SyncTaskExecutionResult.emptyResult();
  }

  private SyncTaskExecutionResult doMultiPartInit(URI uri) throws IOException {
    Path filePath = new Path(uri);
    FileSystem fs = FileSystem.get(uri, conf);
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, conf);
    UploadHandle uploadHandle = mpu.initialize(filePath);
    return new SyncTaskExecutionResult(uploadHandle.bytes(), 0L);
  }

  private SyncTaskExecutionResult doMultiPartComplete(URI uri,
      List<ExtendedBlock> blocks, ByteBuffer uploadHandle, List<ByteBuffer> partHandles)
      throws IOException {
    Path filePath = new Path(uri);

    Map<Integer, PartHandle> partList = IntStream
        .range(0, partHandles.size())
        .mapToObj(i -> Pair.of(i + 1, BBPartHandle.from(
            partHandles.get(i))))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    FileSystem fs = FileSystem.get(uri, conf);
    MultipartUploader mpu = MultipartUploaderFactory.get(fs, conf);
    PathHandle etag = mpu.complete(filePath, partList,
        BBUploadHandle.from(uploadHandle));

    // The complete has an etag but doesn't actually transmit any bytes.
    return new SyncTaskExecutionResult(etag.bytes(), 0L);
  }

  private SyncTaskExecutionResult doCreateDirectory(URI uri)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);

    /*
    If this is the root directory, the URI strips out the initial slash.
    Converting it to a path does not convert it back.
     */
    if (!uri.getPath().isEmpty()) {
      fs.mkdirs(new Path(uri), FsPermission.getDefault());
    }
    return SyncTaskExecutionResult.emptyResult();
  }

  private SyncTaskExecutionResult doDeleteFile(URI uri, List<Block> blocks)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    fs.delete(new Path(uri), false);
    return SyncTaskExecutionResult.emptyResult();
  }

  private SyncTaskExecutionResult doDeleteDirectory(URI uri)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    fs.delete(new Path(uri), true);
    return SyncTaskExecutionResult.emptyResult();
  }

  private SyncTaskExecutionResult doRenameDirectory(URI uri, URI renamedTo)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path renamedToPath = new Path(renamedTo);
    fs.rename(new Path(uri), renamedToPath);
    return SyncTaskExecutionResult.emptyResult();
  }

  private SyncTaskExecutionResult doRename(URI uri, URI renamedTo)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path renamedToPath = new Path(renamedTo);
    fs.rename(new Path(uri), renamedToPath);
    return SyncTaskExecutionResult.emptyResult();
  }
}




