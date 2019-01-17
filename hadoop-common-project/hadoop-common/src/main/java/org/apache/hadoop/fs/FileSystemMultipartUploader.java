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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Charsets;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
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
public class FileSystemMultipartUploader extends MultipartUploader {

  private final FileSystem fs;

  public FileSystemMultipartUploader(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public UploadHandle initialize(Path filePath) throws IOException {
    Path collectorPath = createCollectorPath(filePath);
    fs.mkdirs(collectorPath, FsPermission.getDirDefault());

    ByteBuffer byteBuffer = ByteBuffer.wrap(
        collectorPath.toString().getBytes(Charsets.UTF_8));
    return BBUploadHandle.from(byteBuffer);
  }

  @Override
  public PartHandle putPart(Path filePath, InputStream inputStream,
      int partNumber, UploadHandle uploadId, long lengthInBytes)
      throws IOException {
    checkPutArguments(filePath, inputStream, partNumber, uploadId,
        lengthInBytes);
    byte[] uploadIdByteArray = uploadId.toByteArray();
    checkUploadId(uploadIdByteArray);
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));
    Path partPath =
        mergePaths(collectorPath, mergePaths(new Path(Path.SEPARATOR),
            new Path(Integer.toString(partNumber) + ".part")));
    try(FSDataOutputStream fsDataOutputStream =
            fs.createFile(partPath).build()) {
      IOUtils.copy(inputStream, fsDataOutputStream, 4096);
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
    for (Path p: partHandles) {
      totalLen += fs.getFileStatus(p).getLen();
    }
    return totalLen;
  }

  @Override
  @SuppressWarnings("deprecation") // rename w/ OVERWRITE
  public PathHandle complete(Path filePath, Map<Integer, PartHandle> handleMap,
      UploadHandle multipartUploadId) throws IOException {

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
      fs.rename(filePathInsideCollector, filePath, Options.Rename.OVERWRITE);
    }
    fs.delete(collectorPath, true);
    return getPathHandle(filePath);
  }

  @Override
  public void abort(Path filePath, UploadHandle uploadId) throws IOException {
    byte[] uploadIdByteArray = uploadId.toByteArray();
    checkUploadId(uploadIdByteArray);
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));

    // force a check for a file existing; raises FNFE if not found
    fs.getFileStatus(collectorPath);
    fs.delete(collectorPath, true);
  }

  /**
   * Factory for creating MultipartUploaderFactory objects for file://
   * filesystems.
   */
  public static class Factory extends MultipartUploaderFactory {
    protected MultipartUploader createMultipartUploader(FileSystem fs,
        Configuration conf) {
      if (fs.getScheme().equals("file")) {
        return new FileSystemMultipartUploader(fs);
      }
      return null;
    }
  }
}
