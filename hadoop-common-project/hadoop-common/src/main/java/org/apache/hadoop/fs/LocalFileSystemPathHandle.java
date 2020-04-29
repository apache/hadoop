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

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.fs.FSProtos.LocalFileSystemPathHandleProto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/**
 * Opaque handle to an entity in a FileSystem.
 */
public class LocalFileSystemPathHandle implements PathHandle {

  private final String path;
  private final Long mtime;

  public LocalFileSystemPathHandle(String path, Optional<Long> mtime) {
    this.path = path;
    this.mtime = mtime.orElse(null);
  }

  public LocalFileSystemPathHandle(ByteBuffer bytes) throws IOException {
    if (null == bytes) {
      throw new IOException("Missing PathHandle");
    }
    LocalFileSystemPathHandleProto p =
        LocalFileSystemPathHandleProto.parseFrom(ByteString.copyFrom(bytes));
    path = p.hasPath()   ? p.getPath()  : null;
    mtime = p.hasMtime() ? p.getMtime() : null;
  }

  public String getPath() {
    return path;
  }

  public void verify(FileStatus stat) throws InvalidPathHandleException {
    if (null == stat) {
      throw new InvalidPathHandleException("Could not resolve handle");
    }
    if (mtime != null && mtime != stat.getModificationTime()) {
      throw new InvalidPathHandleException("Content changed");
    }
  }

  @Override
  public ByteBuffer bytes() {
    LocalFileSystemPathHandleProto.Builder b =
        LocalFileSystemPathHandleProto.newBuilder();
    b.setPath(path);
    if (mtime != null) {
      b.setMtime(mtime);
    }
    return b.build().toByteString().asReadOnlyByteBuffer();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LocalFileSystemPathHandle that = (LocalFileSystemPathHandle) o;
    return Objects.equals(path, that.path) &&
        Objects.equals(mtime, that.mtime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, mtime);
  }

  @Override
  public String toString() {
    return "LocalFileSystemPathHandle{" +
        "path='" + path + '\'' +
        ", mtime=" + mtime +
        '}';
  }

}
