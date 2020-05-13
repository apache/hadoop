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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.InvalidPathHandleException;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsPathHandleProto;

import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * Opaque handle to an entity in HDFS.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HdfsPathHandle implements PathHandle {

  private static final long serialVersionUID = 0xc53087a5428L;

  private final String path;
  private final Long mtime;
  private final Long inodeId;

  public HdfsPathHandle(String path,
      Optional<Long> inodeId, Optional<Long> mtime) {
    this.path = path;
    this.mtime = mtime.orElse(null);
    this.inodeId = inodeId.orElse(null);
  }

  public HdfsPathHandle(ByteBuffer bytes) throws IOException {
    if (null == bytes) {
      throw new IOException("Missing PathHandle");
    }
    HdfsPathHandleProto p =
        HdfsPathHandleProto.parseFrom(ByteString.copyFrom(bytes));
    path = p.getPath();
    mtime   = p.hasMtime()   ? p.getMtime()   : null;
    inodeId = p.hasInodeId() ? p.getInodeId() : null;
  }

  public String getPath() {
    return path;
  }

  public void verify(HdfsLocatedFileStatus stat)
      throws InvalidPathHandleException {
    if (null == stat) {
      throw new InvalidPathHandleException("Could not resolve handle");
    }
    if (mtime != null && mtime != stat.getModificationTime()) {
      throw new InvalidPathHandleException("Content changed");
    }
    if (inodeId != null && inodeId != stat.getFileId()) {
      throw new InvalidPathHandleException("Wrong file");
    }
  }

  @Override
  public ByteBuffer bytes() {
    HdfsPathHandleProto.Builder b = HdfsPathHandleProto.newBuilder();
    b.setPath(path);
    if (inodeId != null) {
      b.setInodeId(inodeId);
    }
    if (mtime != null) {
      b.setMtime(mtime);
    }
    return b.build().toByteString().asReadOnlyByteBuffer();
  }

  @Override
  public boolean equals(Object other) {
    if (null == other) {
      return false;
    }
    if (!HdfsPathHandle.class.equals(other.getClass())) {
      // require exact match
      return false;
    }
    HdfsPathHandle o = (HdfsPathHandle)other;
    return getPath().equals(o.getPath());
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ")
        .append("\"path\" : \"").append(path).append("\"");
    if (inodeId != null) {
      sb.append(",\"inodeId\" : ").append(inodeId);
    }
    if (mtime != null) {
      sb.append(",\"mtime\" : ").append(mtime);
    }
    sb.append(" }");
    return sb.toString();
  }

}
