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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.util.LongBitFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

public class FlatINodeFileFeature extends FlatINode.Feature {
  private FlatINodeFileFeature(ByteBuffer data) {
    super(data);
  }
  private FlatINodeFileFeature(ByteString data) {
    super(data);
  }

  private static final int SIZEOF_BLOCK = 24;

  public static FlatINodeFileFeature wrap(ByteString v) {
    return new FlatINodeFileFeature(v);
  }

  public static FlatINodeFileFeature wrap(ByteBuffer v) {
    return new FlatINodeFileFeature(v);
  }

  private enum Header {
    PREFERRED_BLOCK_SIZE(null, 47, 1),
    REPLICATION(PREFERRED_BLOCK_SIZE, 12, 1),
    STORAGE_POLICY_ID(REPLICATION, BlockStoragePolicySuite.ID_BIT_LENGTH,
      0),
    IN_CONSTRUCTION(STORAGE_POLICY_ID, 1, 0);

    private final LongBitFormat BITS;
    Header(Header prev, int length, long min) {
      BITS = new LongBitFormat(name(), prev == null ? null : prev.BITS, length,
        min);
    }

    static long get(Header h, long bits) {
      return h.BITS.retrieve(bits);
    }

    static long build(long blockSize, int replication,
                      byte storagePolicyId, boolean inConstruction) {
      long v = 0;
      v = PREFERRED_BLOCK_SIZE.BITS.combine(blockSize, v);
      v = REPLICATION.BITS.combine(replication, v);
      v = STORAGE_POLICY_ID.BITS.combine(storagePolicyId, v);
      v = IN_CONSTRUCTION.BITS.combine(inConstruction ? 1 : 0, v);
      return v;
    }
  }

  private long header() {
    return data.getLong(0);
  }

  private boolean isInConstruction() {
    return Header.IN_CONSTRUCTION.BITS.retrieve(header()) != 0;
  }

  public int numBlocks() {
    return data.getInt(Encoding.SIZEOF_LONG);
  }

  public long blockSize() {
    return Header.get(Header.PREFERRED_BLOCK_SIZE, header());
  }

  public short replication() {
    return (short) Header.get(Header.REPLICATION, header());
  }

  public byte storagePolicyId() {
    return (byte) Header.get(Header.STORAGE_POLICY_ID, header());
  }

  public boolean inConstruction() {
    return Header.get(Header.IN_CONSTRUCTION, header()) != 0;
  }

  public Iterable<Block> blocks() {
    final int numBlocks = numBlocks();
    return new Iterable<Block>() {
      @Override
      public Iterator<Block> iterator() {
        return new Iterator<Block>() {
          private int i;
          @Override
          public boolean hasNext() {
            return i < numBlocks;
          }

          @Override
          public Block next() {
            int off = offsetOfBlock(i);
            Block bi = readBlock(off);
            ++i;
            return bi;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  long fileSize() {
    long size = 0;
    for (Block b : blocks()) {
      size += b.getNumBytes();
    }
    return size;
  }

  public Block lastBlock() {
    return numBlocks() == 0 ? null : block(numBlocks() - 1);
  }

  public Block penultimateBlock() {
    return numBlocks() >= 2 ? block(numBlocks() - 2) : null;
  }

  public Block block(int index) {
    Preconditions.checkArgument(0 <= index && index < numBlocks());
    int off = offsetOfBlock(index);
    return readBlock(off);
  }

  private Block readBlock(int off) {
    return new Block(data.getLong(off), data.getLong(off + Encoding
      .SIZEOF_LONG), data.getLong(off + 2 * Encoding.SIZEOF_LONG));
  }

  private static int offsetOfBlock(int index) {
    return 2 * Encoding.SIZEOF_LONG + index * SIZEOF_BLOCK;
  }

  private int offsetOfClientName() {
    return offsetOfBlock(numBlocks());
  }

  private int lengthOfClientName() {
    assert isInConstruction();
    return Encoding.readRawVarint32(data, offsetOfClientName());
  }



  public String clientName() {
    if (!isInConstruction()) {
      return null;
    }
    int off = offsetOfClientName();
    return Encoding.readString((ByteBuffer) data.slice().position(off));
  }

  public String clientMachine() {
    if (!isInConstruction()) {
      return null;
    }

    int off = offsetOfClientName() + Encoding.computeArraySize
      (lengthOfClientName());
    return Encoding.readString((ByteBuffer) data.slice().position(off));
  }

  public static class Builder {
    private long blockSize;
    private int replication;
    private boolean inConstruction;
    private byte storagePolicyId;
    private ArrayList<Block> blocks = new ArrayList<>();
    private ByteString clientName;
    private ByteString clientMachine;

    public Builder blockSize(long blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public Builder replication(int replication) {
      this.replication = replication;
      return this;
    }

    public Builder storagePolicyId(byte storagePolicyId) {
      this.storagePolicyId = storagePolicyId;
      return this;
    }

    public Builder inConstruction(boolean inConstruction) {
      this.inConstruction = inConstruction;
      return this;
    }

    public boolean inConstruction() {
      return inConstruction;
    }

    public Builder addBlock(Block block) {
      this.blocks.add(block);
      return this;
    }

    public Builder block(int index, Block b) {
      blocks.set(index, b);
      return this;
    }

    public ArrayList<Block> blocks() {
      return blocks;
    }

    public Builder clearBlock() {
      blocks.clear();
      return this;
    }

    public Builder clientName(String clientName) {
      this.clientName = clientName == null
          ? null : ByteString.copyFromUtf8(clientName);
      return this;
    }

    public Builder clientMachine(String clientMachine) {
      this.clientMachine = clientMachine == null
          ? null : ByteString.copyFromUtf8(clientMachine);
      return this;
    }

    public ByteString build() {
      Preconditions.checkState(!inConstruction || (clientName != null &&
        clientMachine != null));
      int size = Encoding.SIZEOF_LONG * 2 + 3 * Encoding.SIZEOF_LONG * blocks
        .size();
      if (inConstruction) {
        size += Encoding.computeArraySize(clientName.size());
        size += Encoding.computeArraySize(clientMachine.size());
      }
      byte[] res = new byte[size];
      CodedOutputStream o = CodedOutputStream.newInstance(res);
      try {
        o.writeFixed64NoTag(Header.build(blockSize, replication,
          storagePolicyId, inConstruction));
        o.writeFixed64NoTag(blocks.size());
        for (Block b : blocks) {
          o.writeFixed64NoTag(b.getBlockId());
          o.writeFixed64NoTag(b.getNumBytes());
          o.writeFixed64NoTag(b.getGenerationStamp());
        }
        if (inConstruction) {
          o.writeBytesNoTag(clientName);
          o.writeBytesNoTag(clientMachine);
        }
        o.flush();
      } catch (IOException ignored) {
      }
      return ByteString.copyFrom(res);
    }

    Builder mergeFrom(FlatINodeFileFeature o) {
      blockSize(o.blockSize()).replication(o.replication())
        .storagePolicyId(o.storagePolicyId())
        .inConstruction(o.inConstruction());
      blocks = Lists.newArrayList(o.blocks());
      if (o.isInConstruction()) {
        clientName(o.clientName()).clientMachine(o.clientMachine());
      }
      return this;
    }
  }
}
