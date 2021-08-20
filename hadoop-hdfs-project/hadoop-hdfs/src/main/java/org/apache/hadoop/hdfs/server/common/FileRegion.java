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
package org.apache.hadoop.hdfs.server.common;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.GRANDFATHER_GENERATION_STAMP;

/**
 * This class is used to represent provided blocks that are file regions,
 * i.e., can be described using (path, offset, length).
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FileRegion implements BlockAlias {

  private final Block block;
  private final ProvidedStorageLocation providedStorageLocation;

  public FileRegion(long blockId, Path path, long offset,
      long length, long genStamp) {
    this(blockId, path, offset, length, genStamp, new byte[0]);
  }

  public FileRegion(long blockId, Path path, long offset,
                    long length, long genStamp, byte[] nonce) {
    this(new Block(blockId, length, genStamp),
            new ProvidedStorageLocation(path, offset, length, nonce));
  }

  public FileRegion(long blockId, Path path, long offset, long length) {
    this(blockId, path, offset, length,
        GRANDFATHER_GENERATION_STAMP);
  }

  public FileRegion(Block block,
      ProvidedStorageLocation providedStorageLocation) {
    this.block = block;
    this.providedStorageLocation = providedStorageLocation;
  }

  public Block getBlock() {
    return block;
  }

  public ProvidedStorageLocation getProvidedStorageLocation() {
    return providedStorageLocation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FileRegion that = (FileRegion) o;

    return block.equals(that.block) &&
        providedStorageLocation.equals(that.providedStorageLocation);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(block)
        .append(providedStorageLocation).toHashCode();
  }

  public static FileRegion fromProtoBufBytes(byte[] bytes)
      throws IOException {
    AliasMapProtocolProtos.KeyValueProto proto =
        AliasMapProtocolProtos.KeyValueProto.parseFrom(bytes);
    return PBHelper.convert(proto);
  }

  public static byte[] toProtoBufBytes(FileRegion fr) throws IOException {
    AliasMapProtocolProtos.KeyValueProto proto = PBHelper.convert(fr);
    ByteArrayOutputStream blockOutputStream = new ByteArrayOutputStream();
    proto.writeTo(blockOutputStream);
    return blockOutputStream.toByteArray();
  }
}
