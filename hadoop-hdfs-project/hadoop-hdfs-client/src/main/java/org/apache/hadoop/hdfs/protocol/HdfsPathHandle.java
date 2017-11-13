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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsPathHandleProto;

import com.google.protobuf.ByteString;

/**
 * Opaque handle to an entity in HDFS.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HdfsPathHandle implements PathHandle {

  private static final long serialVersionUID = 0xc5308795428L;

  private final long inodeId;

  public HdfsPathHandle(HdfsFileStatus hstat) {
    this(hstat.getFileId());
  }

  public HdfsPathHandle(long inodeId) {
    this.inodeId = inodeId;
  }

  public HdfsPathHandle(ByteBuffer bytes) throws IOException {
    if (null == bytes) {
      throw new IOException("Missing PathHandle");
    }
    HdfsPathHandleProto p =
        HdfsPathHandleProto.parseFrom(ByteString.copyFrom(bytes));
    inodeId = p.getInodeId();
  }

  public long getInodeId() {
    return inodeId;
  }

  @Override
  public ByteBuffer bytes() {
    return HdfsPathHandleProto.newBuilder()
      .setInodeId(getInodeId())
      .build()
      .toByteString()
      .asReadOnlyByteBuffer();
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
    return getInodeId() == o.getInodeId();
  }

  @Override
  public int hashCode() {
    return Long.hashCode(inodeId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ");
    sb.append("inodeId : ").append(Long.toString(getInodeId()));
    sb.append(" }");
    return sb.toString();
  }

}
