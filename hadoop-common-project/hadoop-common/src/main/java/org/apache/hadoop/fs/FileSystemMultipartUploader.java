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

import com.google.common.base.Charsets;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A MultipartUploader that uses the basic FileSystem commands.
 * This is done in three stages:
 * Init - create a temp _multipart directory.
 * PutPart - copying the individual parts of the file to the temp directory.
 * Complete - use {@link FileSystem#concat} to merge the files; and then delete
 * the temp directory.
 */
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

    byte[] uploadIdByteArray = uploadId.toByteArray();
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));
    Path partPath =
        Path.mergePaths(collectorPath, Path.mergePaths(new Path(Path.SEPARATOR),
            new Path(Integer.toString(partNumber) + ".part")));
    FSDataOutputStreamBuilder outputStream = fs.createFile(partPath);
    FSDataOutputStream fsDataOutputStream = outputStream.build();
    IOUtils.copy(inputStream, fsDataOutputStream, 4096);
    fsDataOutputStream.close();
    return BBPartHandle.from(ByteBuffer.wrap(
        partPath.toString().getBytes(Charsets.UTF_8)));
  }

  private Path createCollectorPath(Path filePath) {
    return Path.mergePaths(filePath.getParent(),
        Path.mergePaths(new Path(filePath.getName().split("\\.")[0]),
            Path.mergePaths(new Path("_multipart"),
                new Path(Path.SEPARATOR))));
  }

  @Override
  @SuppressWarnings("deprecation") // rename w/ OVERWRITE
  public PathHandle complete(Path filePath,
      List<Pair<Integer, PartHandle>> handles, UploadHandle multipartUploadId)
      throws IOException {
    handles.sort(Comparator.comparing(Pair::getKey));
    List<Path> partHandles = handles
        .stream()
        .map(pair -> {
          byte[] byteArray = pair.getValue().toByteArray();
          return new Path(new String(byteArray, 0, byteArray.length,
              Charsets.UTF_8));
        })
        .collect(Collectors.toList());

    Path collectorPath = createCollectorPath(filePath);
    Path filePathInsideCollector = Path.mergePaths(collectorPath,
        new Path(Path.SEPARATOR + filePath.getName()));
    fs.create(filePathInsideCollector).close();
    fs.concat(filePathInsideCollector,
        partHandles.toArray(new Path[handles.size()]));
    fs.rename(filePathInsideCollector, filePath, Options.Rename.OVERWRITE);
    fs.delete(collectorPath, true);
    FileStatus status = fs.getFileStatus(filePath);
    return fs.getPathHandle(status);
  }

  @Override
  public void abort(Path filePath, UploadHandle uploadId) throws IOException {
    byte[] uploadIdByteArray = uploadId.toByteArray();
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));
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
