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

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;
import org.apache.hadoop.thirdparty.protobuf.WireFormat;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class BlockListAsLongs implements Iterable<BlockReportReplica> {
  private final static int CHUNK_SIZE = 64*1024; // 64K
  private static long[] EMPTY_LONGS = new long[]{0, 0};

  public static BlockListAsLongs EMPTY = new BlockListAsLongs() {
    @Override
    public int getNumberOfBlocks() {
      return 0;
    }
    @Override
    public ByteString getBlocksBuffer() {
      return ByteString.EMPTY;
    }
    @Override
    public long[] getBlockListAsLongs() {
      return EMPTY_LONGS;
    }
    @Override
    public Iterator<BlockReportReplica> iterator() {
      return Collections.emptyIterator();
    }
  };

  /**
   * Prepare an instance to in-place decode the given ByteString buffer.
   * @param numBlocks - blocks in the buffer
   * @param blocksBuf - ByteString encoded varints
   * @param maxDataLength - maximum allowable data size in protobuf message
   * @return BlockListAsLongs
   */
  public static BlockListAsLongs decodeBuffer(final int numBlocks,
      final ByteString blocksBuf, final int maxDataLength) {
    return new BufferDecoder(numBlocks, blocksBuf, maxDataLength);
  }

  /**
   * Prepare an instance to in-place decode the given ByteString buffers.
   * @param numBlocks - blocks in the buffers
   * @param blocksBufs - list of ByteString encoded varints
   * @return BlockListAsLongs
   */
  @VisibleForTesting
  public static BlockListAsLongs decodeBuffers(final int numBlocks,
      final List<ByteString> blocksBufs) {
    return decodeBuffers(numBlocks, blocksBufs,
        IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  /**
   * Prepare an instance to in-place decode the given ByteString buffers.
   * @param numBlocks - blocks in the buffers
   * @param blocksBufs - list of ByteString encoded varints
   * @param maxDataLength - maximum allowable data size in protobuf message
   * @return BlockListAsLongs
   */
  public static BlockListAsLongs decodeBuffers(final int numBlocks,
      final List<ByteString> blocksBufs, final int maxDataLength) {
    // this doesn't actually copy the data
    return decodeBuffer(numBlocks, ByteString.copyFrom(blocksBufs),
        maxDataLength);
  }

  /**
   * Prepare an instance to in-place decode the given list of Longs.  Note
   * it's much more efficient to decode ByteString buffers and only exists
   * for compatibility.
   * @param blocksList - list of longs
   * @return BlockListAsLongs
   */
  public static BlockListAsLongs decodeLongs(List<Long> blocksList) {
    return decodeLongs(blocksList, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  /**
   * Prepare an instance to in-place decode the given list of Longs.  Note
   * it's much more efficient to decode ByteString buffers and only exists
   * for compatibility.
   * @param blocksList - list of longs
   * @param maxDataLength - maximum allowable data size in protobuf message
   * @return BlockListAsLongs
   */
  public static BlockListAsLongs decodeLongs(List<Long> blocksList,
      int maxDataLength) {
    return blocksList.isEmpty() ? EMPTY :
        new LongsDecoder(blocksList, maxDataLength);
  }

  /**
   * Prepare an instance to encode the collection of replicas into an
   * efficient ByteString.
   * @param replicas - replicas to encode
   * @return BlockListAsLongs
   */
  @VisibleForTesting
  public static BlockListAsLongs encode(
      final Collection<? extends Replica> replicas) {
    BlockListAsLongs.Builder builder = builder(IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    for (Replica replica : replicas) {
      builder.add(replica);
    }
    return builder.build();
  }

  public static BlockListAsLongs readFrom(InputStream is, int maxDataLength)
      throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(is);
    if (maxDataLength != IPC_MAXIMUM_DATA_LENGTH_DEFAULT) {
      cis.setSizeLimit(maxDataLength);
    }
    int numBlocks = -1;
    ByteString blocksBuf = null;
    while (!cis.isAtEnd()) {
      int tag = cis.readTag();
      int field = WireFormat.getTagFieldNumber(tag);
      switch(field) {
        case 0:
          break;
        case 1:
          numBlocks = (int)cis.readInt32();
          break;
        case 2:
          blocksBuf = cis.readBytes();
          break;
        default:
          cis.skipField(tag);
          break;
      }
    }
    if (numBlocks != -1 && blocksBuf != null) {
      return decodeBuffer(numBlocks, blocksBuf, maxDataLength);
    }
    return null;
  }

  public void writeTo(OutputStream os) throws IOException {
    CodedOutputStream cos = CodedOutputStream.newInstance(os);
    cos.writeInt32(1, getNumberOfBlocks());
    cos.writeBytes(2, getBlocksBuffer());
    cos.flush();
  }

  @VisibleForTesting
  public static Builder builder() {
    return builder(IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  public static Builder builder(int maxDataLength) {
    return new BlockListAsLongs.Builder(maxDataLength);
  }

  /**
   * The number of blocks
   * @return - the number of blocks
   */
  abstract public int getNumberOfBlocks();

  /**
   * Very efficient encoding of the block report into a ByteString to avoid
   * the overhead of protobuf repeating fields.  Primitive repeating fields
   * require re-allocs of an ArrayList&lt;Long&gt; and the associated (un)boxing
   * overhead which puts pressure on GC.
   * 
   * The structure of the buffer is as follows:
   * - each replica is represented by 4 longs:
   *   blockId, block length, genstamp, replica state
   *
   * @return ByteString encoded block report
   */
  abstract public ByteString getBlocksBuffer();

  /**
   * List of ByteStrings that encode this block report
   *
   * @return ByteStrings
   */
  public List<ByteString> getBlocksBuffers() {
    final ByteString blocksBuf = getBlocksBuffer();
    final List<ByteString> buffers;
    final int size = blocksBuf.size();
    if (size <= CHUNK_SIZE) {
      buffers = Collections.singletonList(blocksBuf);
    } else {
      buffers = new ArrayList<ByteString>();
      for (int pos=0; pos < size; pos += CHUNK_SIZE) {
        // this doesn't actually copy the data
        buffers.add(blocksBuf.substring(pos, Math.min(pos+CHUNK_SIZE, size)));
      }
    }
    return buffers;
  }

  /**
   * Convert block report to old-style list of longs.  Only used to
   * re-encode the block report when the DN detects an older NN. This is
   * inefficient, but in practice a DN is unlikely to be upgraded first
   * 
   * The structure of the array is as follows:
   * 0: the length of the finalized replica list;
   * 1: the length of the under-construction replica list;
   * - followed by finalized replica list where each replica is represented by
   *   3 longs: one for the blockId, one for the block length, and one for
   *   the generation stamp;
   * - followed by the invalid replica represented with three -1s;
   * - followed by the under-construction replica list where each replica is
   *   represented by 4 longs: three for the block id, length, generation 
   *   stamp, and the fourth for the replica state.
   * @return list of longs
   */
  abstract public long[] getBlockListAsLongs();

  /**
   * Returns a singleton iterator over blocks in the block report.  Do not
   * add the returned blocks to a collection.
   * @return Iterator
   */
  abstract public Iterator<BlockReportReplica> iterator();

  public static class Builder {
    private final ByteString.Output out;
    private final CodedOutputStream cos;
    private int numBlocks = 0;
    private int numFinalized = 0;
    private final int maxDataLength;

    Builder(int maxDataLength) {
      out = ByteString.newOutput(64*1024);
      cos = CodedOutputStream.newInstance(out);
      this.maxDataLength = maxDataLength;
    }

    public void add(Replica replica) {
      try {
        // zig-zag to reduce size of legacy blocks
        cos.writeSInt64NoTag(replica.getBlockId());
        cos.writeUInt64NoTag(replica.getBytesOnDisk());
        cos.writeUInt64NoTag(replica.getGenerationStamp());
        ReplicaState state = replica.getState();
        // although state is not a 64-bit value, using a long varint to
        // allow for future use of the upper bits
        cos.writeUInt64NoTag(state.getValue());
        if (state == ReplicaState.FINALIZED) {
          numFinalized++;
        }
        numBlocks++;
      } catch (IOException ioe) {
        // shouldn't happen, ByteString.Output doesn't throw IOE
        throw new IllegalStateException(ioe);
      }
    }

    public int getNumberOfBlocks() {
      return numBlocks;
    }
    
    public BlockListAsLongs build() {
      try {
        cos.flush();
      } catch (IOException ioe) {
        // shouldn't happen, ByteString.Output doesn't throw IOE
        throw new IllegalStateException(ioe);
      }
      return new BufferDecoder(numBlocks, numFinalized, out.toByteString(),
          maxDataLength);
    }
  }

  // decode new-style ByteString buffer based block report
  private static class BufferDecoder extends BlockListAsLongs {
    // reserve upper bits for future use.  decoding masks off these bits to
    // allow compatibility for the current through future release that may
    // start using the bits
    private static long NUM_BYTES_MASK = (-1L) >>> (64 - 48);
    private static long REPLICA_STATE_MASK = (-1L) >>> (64 - 4);

    private final ByteString buffer;
    private final int numBlocks;
    private int numFinalized;
    private final int maxDataLength;

    BufferDecoder(final int numBlocks, final ByteString buf,
        final int maxDataLength) {
      this(numBlocks, -1, buf, maxDataLength);
    }

    BufferDecoder(final int numBlocks, final int numFinalized,
        final ByteString buf, final int maxDataLength) {
      this.numBlocks = numBlocks;
      this.numFinalized = numFinalized;
      this.buffer = buf;
      this.maxDataLength = maxDataLength;
    }

    @Override
    public int getNumberOfBlocks() {
      return numBlocks;
    }

    @Override
    public ByteString getBlocksBuffer() {
      return buffer;
    }

    @Override
    public long[] getBlockListAsLongs() {
      // terribly inefficient but only occurs if server tries to transcode
      // an undecoded buffer into longs - ie. it will never happen but let's
      // handle it anyway
      if (numFinalized == -1) {
        int n = 0;
        for (Replica replica : this) {
          if (replica.getState() == ReplicaState.FINALIZED) {
            n++;
          }
        }
        numFinalized = n;
      }
      int numUc = numBlocks - numFinalized;
      int size = 2 + 3*(numFinalized+1) + 4*(numUc);
      long[] longs = new long[size];
      longs[0] = numFinalized;
      longs[1] = numUc;

      int idx = 2;
      int ucIdx = idx + 3*numFinalized;
      // delimiter block
      longs[ucIdx++] = -1;
      longs[ucIdx++] = -1;
      longs[ucIdx++] = -1;

      for (BlockReportReplica block : this) {
        switch (block.getState()) {
          case FINALIZED: {
            longs[idx++] = block.getBlockId();
            longs[idx++] = block.getNumBytes();
            longs[idx++] = block.getGenerationStamp();
            break;
          }
          default: {
            longs[ucIdx++] = block.getBlockId();
            longs[ucIdx++] = block.getNumBytes();
            longs[ucIdx++] = block.getGenerationStamp();
            longs[ucIdx++] = block.getState().getValue();
            break;
          }
        }
      }
      return longs;
    }

    @Override
    public Iterator<BlockReportReplica> iterator() {
      return new Iterator<BlockReportReplica>() {
        final BlockReportReplica block = new BlockReportReplica();
        final CodedInputStream cis = buffer.newCodedInput();
        private int currentBlockIndex = 0;

        {
          if (maxDataLength != IPC_MAXIMUM_DATA_LENGTH_DEFAULT) {
            cis.setSizeLimit(maxDataLength);
          }
        }

        @Override
        public boolean hasNext() {
          return currentBlockIndex < numBlocks;
        }

        @Override
        public BlockReportReplica next() {
          currentBlockIndex++;
          try {
            // zig-zag to reduce size of legacy blocks and mask off bits
            // we don't (yet) understand
            block.setBlockId(cis.readSInt64());
            block.setNumBytes(cis.readRawVarint64() & NUM_BYTES_MASK);
            block.setGenerationStamp(cis.readRawVarint64());
            long state = cis.readRawVarint64() & REPLICA_STATE_MASK;
            block.setState(ReplicaState.getState((int)state));
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
          return block;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  // decode old style block report of longs
  private static class LongsDecoder extends BlockListAsLongs {
    private final long[] values;
    private final int finalizedBlocks;
    private final int numBlocks;
    private final int maxDataLength;

    // set the header
    LongsDecoder(List<Long> values, int maxDataLength) {
      this.values = new long[values.size() - 2];
      for (int i = 2; i < values.size(); i++) {
        this.values[i - 2] = values.get(i);
      }
      this.finalizedBlocks = values.get(0).intValue();
      this.numBlocks = finalizedBlocks + values.get(1).intValue();
      this.maxDataLength = maxDataLength;
    }

    @Override
    public int getNumberOfBlocks() {
      return numBlocks;
    }

    @Override
    public ByteString getBlocksBuffer() {
      Builder builder = builder(maxDataLength);
      for (Replica replica : this) {
        builder.add(replica);
      }
      return builder.build().getBlocksBuffer();
    }

    @Override
    public long[] getBlockListAsLongs() {
      long[] longs = new long[2 + values.length];
      longs[0] = finalizedBlocks;
      longs[1] = numBlocks - finalizedBlocks;
      for (int i = 0; i < values.length; i++) {
        longs[2 + i] = values[i];
      }
      return longs;
    }

    @Override
    public Iterator<BlockReportReplica> iterator() {
      return new Iterator<BlockReportReplica>() {
        private final BlockReportReplica block = new BlockReportReplica();
        private int idx = 0;
        private int currentBlockIndex = 0;

        @Override
        public boolean hasNext() {
          return currentBlockIndex < numBlocks;
        }

        @Override
        public BlockReportReplica next() {
          if (currentBlockIndex == finalizedBlocks) {
            // verify the presence of the delimiter block
            readBlock();
            Preconditions.checkArgument(block.getBlockId() == -1 &&
                                        block.getNumBytes() == -1 &&
                                        block.getGenerationStamp() == -1,
                                        "Invalid delimiter block");
          }

          readBlock();
          if (currentBlockIndex++ < finalizedBlocks) {
            block.setState(ReplicaState.FINALIZED);
          } else {
            block.setState(ReplicaState.getState((int) values[idx++]));
          }
          return block;
        }

        private void readBlock() {
          block.setBlockId(values[idx++]);
          block.setNumBytes(values[idx++]);
          block.setGenerationStamp(values[idx++]);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
  
  @InterfaceAudience.Private
  public static class BlockReportReplica extends Block implements Replica {
    private ReplicaState state;

    private BlockReportReplica() {
    }
    public BlockReportReplica(Block block) {
      super(block);
      if (block instanceof BlockReportReplica) {
        this.state = ((BlockReportReplica)block).getState();
      } else {
        this.state = ReplicaState.FINALIZED;
      }
    }
    public void setState(ReplicaState state) {
      this.state = state;
    }
    @Override
    public ReplicaState getState() {
      return state;
    }
    @Override
    public long getBytesOnDisk() {
      return getNumBytes();
    }
    @Override
    public long getVisibleLength() {
      throw new UnsupportedOperationException();
    }
    @Override
    public String getStorageUuid() {
      throw new UnsupportedOperationException();
    }
    @Override
    public boolean isOnTransientStorage() {
      throw new UnsupportedOperationException();
    }
    @Override
    public FsVolumeSpi getVolume() {
      throw new UnsupportedOperationException();
    }
    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }
    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
