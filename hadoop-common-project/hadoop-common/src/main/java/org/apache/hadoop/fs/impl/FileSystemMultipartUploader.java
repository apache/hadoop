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

package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InternalOperations;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.Path.mergePaths;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * A MultipartUploader that uses the basic FileSystem commands.
 * This is done in three stages:
 * <ul>
 *   <li>Init - create a temp {@code _multipart} directory.</li>
 *   <li>PutPart - copying the individual parts of the file to the temp
 *   directory.</li>
 *   <li>Complete - use {@link FileSystem#concat} to merge the files;
 *   and then delete the temp directory.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemMultipartUploader extends AbstractMultipartUploader {

  private static final Logger LOG = LoggerFactory.getLogger(
      FileSystemMultipartUploader.class);

  private final FileSystem fs;

  private final FileSystemMultipartUploaderBuilder builder;

  private final FsPermission permission;

  private final long blockSize;

  private final Options.ChecksumOpt checksumOpt;

  public FileSystemMultipartUploader(
      final FileSystemMultipartUploaderBuilder builder,
      FileSystem fs) {
    super(builder.getPath());
    this.builder = builder;
    this.fs = fs;
    blockSize = builder.getBlockSize();
    checksumOpt = builder.getChecksumOpt();
    permission = builder.getPermission();
  }

  @Override
  public CompletableFuture<UploadHandle> startUpload(Path filePath)
      throws IOException {
    checkPath(filePath);
    return FutureIOSupport.eval(() -> {
      Path collectorPath = createCollectorPath(filePath);
      fs.mkdirs(collectorPath, FsPermission.getDirDefault());

      ByteBuffer byteBuffer = ByteBuffer.wrap(
          collectorPath.toString().getBytes(Charsets.UTF_8));
      return BBUploadHandle.from(byteBuffer);
    });
  }

  @Override
  public CompletableFuture<PartHandle> putPart(UploadHandle uploadId,
      int partNumber, Path filePath,
      InputStream inputStream,
      long lengthInBytes)
      throws IOException {
    checkPutArguments(filePath, inputStream, partNumber, uploadId,
        lengthInBytes);
    return FutureIOSupport.eval(() -> innerPutPart(filePath,
        inputStream, partNumber, uploadId, lengthInBytes));
  }

  private PartHandle innerPutPart(Path filePath,
      InputStream inputStream,
      int partNumber,
      UploadHandle uploadId,
      long lengthInBytes)
      throws IOException {
    byte[] uploadIdByteArray = uploadId.toByteArray();
    checkUploadId(uploadIdByteArray);
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));
    Path partPath =
        mergePaths(collectorPath, mergePaths(new Path(Path.SEPARATOR),
            new Path(partNumber + ".part")));
    final FSDataOutputStreamBuilder fileBuilder = fs.createFile(partPath);
    if (checksumOpt != null) {
      fileBuilder.checksumOpt(checksumOpt);
    }
    if (permission != null) {
      fileBuilder.permission(permission);
    }
    try (FSDataOutputStream fsDataOutputStream =
             fileBuilder.blockSize(blockSize).build()) {
      IOUtils.copy(inputStream, fsDataOutputStream,
          this.builder.getBufferSize());
    } finally {
      cleanupWithLogger(LOG, inputStream);
    }
    return BBPartHandle.from(ByteBuffer.wrap(
        partPath.toString().getBytes(Charsets.UTF_8)));
  }

  private Path createCollectorPath(Path filePath) {
    String uuid = UUID.randomUUID().toString();
    return mergePaths(filePath.getParent(),
        mergePaths(new Path(filePath.getName().split("\\.")[0]),
            mergePaths(new Path("_multipart_" + uuid),
                new Path(Path.SEPARATOR))));
  }

  private PathHandle getPathHandle(Path filePath) throws IOException {
    FileStatus status = fs.getFileStatus(filePath);
    return fs.getPathHandle(status);
  }

  private long totalPartsLen(List<Path> partHandles) throws IOException {
    long totalLen = 0;
    for (Path p : partHandles) {
      totalLen += fs.getFileStatus(p).getLen();
    }
    return totalLen;
  }

  @Override
  public CompletableFuture<PathHandle> complete(
      UploadHandle uploadId,
      Path filePath,
      Map<Integer, PartHandle> handleMap) throws IOException {

    checkPath(filePath);
    return FutureIOSupport.eval(() ->
        innerComplete(uploadId, filePath, handleMap));
  }

  /**
   * The upload complete operation.
   * @param multipartUploadId the ID of the upload
   * @param filePath path
   * @param handleMap map of handles
   * @return the path handle
   * @throws IOException failure
   */
  private PathHandle innerComplete(
      UploadHandle multipartUploadId, Path filePath,
      Map<Integer, PartHandle> handleMap) throws IOException {

    checkPath(filePath);

    checkUploadId(multipartUploadId.toByteArray());

    checkPartHandles(handleMap);
    List<Map.Entry<Integer, PartHandle>> handles =
        new ArrayList<>(handleMap.entrySet());
    handles.sort(Comparator.comparingInt(Map.Entry::getKey));

    List<Path> partHandles = handles
        .stream()
        .map(pair -> {
          byte[] byteArray = pair.getValue().toByteArray();
          return new Path(new String(byteArray, 0, byteArray.length,
              Charsets.UTF_8));
        })
        .collect(Collectors.toList());

    int count = partHandles.size();
    // built up to identify duplicates -if the size of this set is
    // below that of the number of parts, then there's a duplicate entry.
    Set<Path> values = new HashSet<>(count);
    values.addAll(partHandles);
    Preconditions.checkArgument(values.size() == count,
        "Duplicate PartHandles");
    byte[] uploadIdByteArray = multipartUploadId.toByteArray();
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));

    boolean emptyFile = totalPartsLen(partHandles) == 0;
    if (emptyFile) {
      fs.create(filePath).close();
    } else {
      Path filePathInsideCollector = mergePaths(collectorPath,
          new Path(Path.SEPARATOR + filePath.getName()));
      fs.create(filePathInsideCollector).close();
      fs.concat(filePathInsideCollector,
          partHandles.toArray(new Path[handles.size()]));
      new InternalOperations()
          .rename(fs, filePathInsideCollector, filePath,
              Options.Rename.OVERWRITE);
    }
    fs.delete(collectorPath, true);
    return getPathHandle(filePath);
  }

  @Override
  public CompletableFuture<Void> abort(UploadHandle uploadId,
      Path filePath)
      throws IOException {
    checkPath(filePath);
    byte[] uploadIdByteArray = uploadId.toByteArray();
    checkUploadId(uploadIdByteArray);
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));

    return FutureIOSupport.eval(() -> {
      // force a check for a file existing; raises FNFE if not found
      fs.getFileStatus(collectorPath);
      fs.delete(collectorPath, true);
      return null;
    });
  }

}
