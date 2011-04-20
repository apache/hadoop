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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.Token;

/**
 * Transfer data to/from datanode using a streaming protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTransferProtocol {
  public static final Log LOG = LogFactory.getLog(DataTransferProtocol.class);
  
  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious. 
   */
  /*
   * Version 23:
   *    Changed the protocol methods to use ExtendedBlock instead
   *    of Block.
   */
  public static final int DATA_TRANSFER_VERSION = 23;

  /** Operation */
  public enum Op {
    WRITE_BLOCK((byte)80),
    READ_BLOCK((byte)81),
    READ_METADATA((byte)82),
    REPLACE_BLOCK((byte)83),
    COPY_BLOCK((byte)84),
    BLOCK_CHECKSUM((byte)85),
    TRANSFER_BLOCK((byte)86);

    /** The code for this operation. */
    public final byte code;
    
    private Op(byte code) {
      this.code = code;
    }
    
    private static final int FIRST_CODE = values()[0].code;
    /** Return the object represented by the code. */
    private static Op valueOf(byte code) {
      final int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length? null: values()[i];
    }

    /** Read from in */
    public static Op read(DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.write(code);
    }
  };

  /** Status */
  public enum Status {
    SUCCESS(0),
    ERROR(1),
    ERROR_CHECKSUM(2),
    ERROR_INVALID(3),
    ERROR_EXISTS(4),
    ERROR_ACCESS_TOKEN(5),
    CHECKSUM_OK(6);

    /** The code for this operation. */
    private final int code;
    
    private Status(int code) {
      this.code = code;
    }

    private static final int FIRST_CODE = values()[0].code;
    /** Return the object represented by the code. */
    private static Status valueOf(int code) {
      final int i = code - FIRST_CODE;
      return i < 0 || i >= values().length? null: values()[i];
    }

    /** Read from in */
    public static Status read(DataInput in) throws IOException {
      return valueOf(in.readShort());
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.writeShort(code);
    }

    /** Write to out */
    public void writeOutputStream(OutputStream out) throws IOException {
      out.write(new byte[] {(byte)(code >>> 8), (byte)code});
    }
  };
  
  public enum BlockConstructionStage {
    /** The enumerates are always listed as regular stage followed by the
     * recovery stage. 
     * Changing this order will make getRecoveryStage not working.
     */
    // pipeline set up for block append
    PIPELINE_SETUP_APPEND,
    // pipeline set up for failed PIPELINE_SETUP_APPEND recovery
    PIPELINE_SETUP_APPEND_RECOVERY,
    // data streaming
    DATA_STREAMING,
    // pipeline setup for failed data streaming recovery
    PIPELINE_SETUP_STREAMING_RECOVERY,
    // close the block and pipeline
    PIPELINE_CLOSE,
    // Recover a failed PIPELINE_CLOSE
    PIPELINE_CLOSE_RECOVERY,
    // pipeline set up for block creation
    PIPELINE_SETUP_CREATE,
    // transfer RBW for adding datanodes
    TRANSFER_RBW,
    // transfer Finalized for adding datanodes
    TRANSFER_FINALIZED;
    
    final static private byte RECOVERY_BIT = (byte)1;
    
    /**
     * get the recovery stage of this stage
     */
    public BlockConstructionStage getRecoveryStage() {
      if (this == PIPELINE_SETUP_CREATE) {
        throw new IllegalArgumentException( "Unexpected blockStage " + this);
      } else {
        return values()[ordinal()|RECOVERY_BIT];
      }
    }
    
    private static BlockConstructionStage valueOf(byte code) {
      return code < 0 || code >= values().length? null: values()[code];
    }
    
    /** Read from in */
    private static BlockConstructionStage readFields(DataInput in)
    throws IOException {
      return valueOf(in.readByte());
    }

    /** write to out */
    private void write(DataOutput out) throws IOException {
      out.writeByte(ordinal());
    }
  }    

  /** @deprecated Deprecated at 0.21.  Use Op.WRITE_BLOCK instead. */
  @Deprecated
  public static final byte OP_WRITE_BLOCK = Op.WRITE_BLOCK.code;
  /** @deprecated Deprecated at 0.21.  Use Op.READ_BLOCK instead. */
  @Deprecated
  public static final byte OP_READ_BLOCK = Op.READ_BLOCK.code;
  /** @deprecated As of version 15, OP_READ_METADATA is no longer supported. */
  @Deprecated
  public static final byte OP_READ_METADATA = Op.READ_METADATA.code;
  /** @deprecated Deprecated at 0.21.  Use Op.REPLACE_BLOCK instead. */
  @Deprecated
  public static final byte OP_REPLACE_BLOCK = Op.REPLACE_BLOCK.code;
  /** @deprecated Deprecated at 0.21.  Use Op.COPY_BLOCK instead. */
  @Deprecated
  public static final byte OP_COPY_BLOCK = Op.COPY_BLOCK.code;
  /** @deprecated Deprecated at 0.21.  Use Op.BLOCK_CHECKSUM instead. */
  @Deprecated
  public static final byte OP_BLOCK_CHECKSUM = Op.BLOCK_CHECKSUM.code;


  /** @deprecated Deprecated at 0.21.  Use Status.SUCCESS instead. */
  @Deprecated
  public static final int OP_STATUS_SUCCESS = Status.SUCCESS.code;  
  /** @deprecated Deprecated at 0.21.  Use Status.ERROR instead. */
  @Deprecated
  public static final int OP_STATUS_ERROR = Status.ERROR.code;
  /** @deprecated Deprecated at 0.21.  Use Status.ERROR_CHECKSUM instead. */
  @Deprecated
  public static final int OP_STATUS_ERROR_CHECKSUM = Status.ERROR_CHECKSUM.code;
  /** @deprecated Deprecated at 0.21.  Use Status.ERROR_INVALID instead. */
  @Deprecated
  public static final int OP_STATUS_ERROR_INVALID = Status.ERROR_INVALID.code;
  /** @deprecated Deprecated at 0.21.  Use Status.ERROR_EXISTS instead. */
  @Deprecated
  public static final int OP_STATUS_ERROR_EXISTS = Status.ERROR_EXISTS.code;
  /** @deprecated Deprecated at 0.21.  Use Status.ERROR_ACCESS_TOKEN instead.*/
  @Deprecated
  public static final int OP_STATUS_ERROR_ACCESS_TOKEN = Status.ERROR_ACCESS_TOKEN.code;
  /** @deprecated Deprecated at 0.21.  Use Status.CHECKSUM_OK instead. */
  @Deprecated
  public static final int OP_STATUS_CHECKSUM_OK = Status.CHECKSUM_OK.code;


  /** Sender */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class Sender {
    /** Initialize a operation. */
    public static void op(DataOutputStream out, Op op) throws IOException {
      out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
      op.write(out);
    }

    /** Send OP_READ_BLOCK */
    public static void opReadBlock(DataOutputStream out, ExtendedBlock blk,
        long blockOffset, long blockLen, String clientName,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      op(out, Op.READ_BLOCK);

      blk.writeId(out);
      out.writeLong(blockOffset);
      out.writeLong(blockLen);
      Text.writeString(out, clientName);
      blockToken.write(out);
      out.flush();
    }
    
    /** Send OP_WRITE_BLOCK */
    public static void opWriteBlock(DataOutputStream out, ExtendedBlock blk,
        int pipelineSize, BlockConstructionStage stage, long newGs,
        long minBytesRcvd, long maxBytesRcvd, String client, DatanodeInfo src,
        DatanodeInfo[] targets, Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      op(out, Op.WRITE_BLOCK);

      blk.writeId(out);
      out.writeInt(pipelineSize);
      stage.write(out);
      WritableUtils.writeVLong(out, newGs);
      WritableUtils.writeVLong(out, minBytesRcvd);
      WritableUtils.writeVLong(out, maxBytesRcvd);
      Text.writeString(out, client);

      out.writeBoolean(src != null);
      if (src != null) {
        src.write(out);
      }
      write(out, 1, targets);
      blockToken.write(out);
    }

    /** Send {@link Op#TRANSFER_BLOCK} */
    public static void opTransferBlock(DataOutputStream out, ExtendedBlock blk,
        String client, DatanodeInfo[] targets,
        Token<BlockTokenIdentifier> blockToken) throws IOException {
      op(out, Op.TRANSFER_BLOCK);

      blk.writeId(out);
      Text.writeString(out, client);
      write(out, 0, targets);
      blockToken.write(out);
      out.flush();
    }

    /** Send OP_REPLACE_BLOCK */
    public static void opReplaceBlock(DataOutputStream out,
        ExtendedBlock blk, String storageId, DatanodeInfo src,
        Token<BlockTokenIdentifier> blockToken) throws IOException {
      op(out, Op.REPLACE_BLOCK);

      blk.writeId(out);
      Text.writeString(out, storageId);
      src.write(out);
      blockToken.write(out);
      out.flush();
    }

    /** Send OP_COPY_BLOCK */
    public static void opCopyBlock(DataOutputStream out, ExtendedBlock blk,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      op(out, Op.COPY_BLOCK);

      blk.writeId(out);
      blockToken.write(out);
      out.flush();
    }

    /** Send OP_BLOCK_CHECKSUM */
    public static void opBlockChecksum(DataOutputStream out, ExtendedBlock blk,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException {
      op(out, Op.BLOCK_CHECKSUM);
      
      blk.writeId(out);
      blockToken.write(out);
      out.flush();
    }

    /** Write an array of {@link DatanodeInfo} */
    private static void write(final DataOutputStream out,
        final int start, 
        final DatanodeInfo[] datanodeinfos) throws IOException {
      out.writeInt(datanodeinfos.length - start);
      for (int i = start; i < datanodeinfos.length; i++) {
        datanodeinfos[i].write(out);
      }
    }
  }

  /** Receiver */
  public static abstract class Receiver {
    /** Read an Op.  It also checks protocol version. */
    protected final Op readOp(DataInputStream in) throws IOException {
      final short version = in.readShort();
      if (version != DATA_TRANSFER_VERSION) {
        throw new IOException( "Version Mismatch (Expected: " +
            DataTransferProtocol.DATA_TRANSFER_VERSION  +
            ", Received: " +  version + " )");
      }
      return Op.read(in);
    }

    /** Process op by the corresponding method. */
    protected final void processOp(Op op, DataInputStream in
        ) throws IOException {
      switch(op) {
      case READ_BLOCK:
        opReadBlock(in);
        break;
      case WRITE_BLOCK:
        opWriteBlock(in);
        break;
      case REPLACE_BLOCK:
        opReplaceBlock(in);
        break;
      case COPY_BLOCK:
        opCopyBlock(in);
        break;
      case BLOCK_CHECKSUM:
        opBlockChecksum(in);
        break;
      case TRANSFER_BLOCK:
        opTransferBlock(in);
        break;
      default:
        throw new IOException("Unknown op " + op + " in data stream");
      }
    }

    /** Receive OP_READ_BLOCK */
    private void opReadBlock(DataInputStream in) throws IOException {
      final ExtendedBlock blk = new ExtendedBlock();
      blk.readId(in);
      final long offset = in.readLong();
      final long length = in.readLong();
      final String client = Text.readString(in);
      final Token<BlockTokenIdentifier> blockToken = readBlockToken(in);

      opReadBlock(in, blk, offset, length, client, blockToken);
    }

    /**
     * Abstract OP_READ_BLOCK method. Read a block.
     */
    protected abstract void opReadBlock(DataInputStream in, ExtendedBlock blk,
        long offset, long length, String client,
        Token<BlockTokenIdentifier> blockToken) throws IOException;
    
    /** Receive OP_WRITE_BLOCK */
    private void opWriteBlock(DataInputStream in) throws IOException {
      final ExtendedBlock blk = new ExtendedBlock();
      blk.readId(in);
      final int pipelineSize = in.readInt(); // num of datanodes in entire pipeline
      final BlockConstructionStage stage = 
        BlockConstructionStage.readFields(in);
      final long newGs = WritableUtils.readVLong(in);
      final long minBytesRcvd = WritableUtils.readVLong(in);
      final long maxBytesRcvd = WritableUtils.readVLong(in);
      final String client = Text.readString(in); // working on behalf of this client
      final DatanodeInfo src = in.readBoolean()? DatanodeInfo.read(in): null;

      final DatanodeInfo targets[] = readDatanodeInfos(in);
      final Token<BlockTokenIdentifier> blockToken = readBlockToken(in);

      opWriteBlock(in, blk, pipelineSize, stage,
          newGs, minBytesRcvd, maxBytesRcvd, client, src, targets, blockToken);
    }

    /**
     * Abstract OP_WRITE_BLOCK method. 
     * Write a block.
     */
    protected abstract void opWriteBlock(DataInputStream in, ExtendedBlock blk,
        int pipelineSize, BlockConstructionStage stage, long newGs,
        long minBytesRcvd, long maxBytesRcvd, String client, DatanodeInfo src,
        DatanodeInfo[] targets, Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Receive {@link Op#TRANSFER_BLOCK} */
    private void opTransferBlock(DataInputStream in) throws IOException {
      final ExtendedBlock blk = new ExtendedBlock();
      blk.readId(in);
      final String client = Text.readString(in);
      final DatanodeInfo targets[] = readDatanodeInfos(in);
      final Token<BlockTokenIdentifier> blockToken = readBlockToken(in);

      opTransferBlock(in, blk, client, targets, blockToken);
    }

    /**
     * Abstract {@link Op#TRANSFER_BLOCK} method.
     * For {@link BlockConstructionStage#TRANSFER_RBW}
     * or {@link BlockConstructionStage#TRANSFER_FINALIZED}.
     */
    protected abstract void opTransferBlock(DataInputStream in, ExtendedBlock blk,
        String client, DatanodeInfo[] targets,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Receive OP_REPLACE_BLOCK */
    private void opReplaceBlock(DataInputStream in) throws IOException {
      final ExtendedBlock blk = new ExtendedBlock();
      blk.readId(in);
      final String sourceId = Text.readString(in); // read del hint
      final DatanodeInfo src = DatanodeInfo.read(in); // read proxy source
      final Token<BlockTokenIdentifier> blockToken = readBlockToken(in);

      opReplaceBlock(in, blk, sourceId, src, blockToken);
    }

    /**
     * Abstract OP_REPLACE_BLOCK method.
     * It is used for balancing purpose; send to a destination
     */
    protected abstract void opReplaceBlock(DataInputStream in,
        ExtendedBlock blk, String sourceId, DatanodeInfo src,
        Token<BlockTokenIdentifier> blockToken) throws IOException;

    /** Receive OP_COPY_BLOCK */
    private void opCopyBlock(DataInputStream in) throws IOException {
      final ExtendedBlock blk = new ExtendedBlock();
      blk.readId(in);
      final Token<BlockTokenIdentifier> blockToken = readBlockToken(in);

      opCopyBlock(in, blk, blockToken);
    }

    /**
     * Abstract OP_COPY_BLOCK method. It is used for balancing purpose; send to
     * a proxy source.
     */
    protected abstract void opCopyBlock(DataInputStream in, ExtendedBlock blk,
        Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Receive OP_BLOCK_CHECKSUM */
    private void opBlockChecksum(DataInputStream in) throws IOException {
      final ExtendedBlock blk = new ExtendedBlock();
      blk.readId(in);
      final Token<BlockTokenIdentifier> blockToken = readBlockToken(in);

      opBlockChecksum(in, blk, blockToken);
    }

    /**
     * Abstract OP_BLOCK_CHECKSUM method.
     * Get the checksum of a block 
     */
    protected abstract void opBlockChecksum(DataInputStream in,
        ExtendedBlock blk, Token<BlockTokenIdentifier> blockToken)
        throws IOException;

    /** Read an array of {@link DatanodeInfo} */
    private static DatanodeInfo[] readDatanodeInfos(final DataInputStream in
        ) throws IOException {
      final int n = in.readInt();
      if (n < 0) {
        throw new IOException("Mislabelled incoming datastream: "
            + n + " = n < 0");
      }
      final DatanodeInfo[] datanodeinfos= new DatanodeInfo[n];
      for (int i = 0; i < datanodeinfos.length; i++) {
        datanodeinfos[i] = DatanodeInfo.read(in);
      }
      return datanodeinfos;
    }

    /** Read an AccessToken */
    static private Token<BlockTokenIdentifier> readBlockToken(DataInputStream in
        ) throws IOException {
      final Token<BlockTokenIdentifier> t = new Token<BlockTokenIdentifier>();
      t.readFields(in);
      return t; 
    }
  }
  
  /** reply **/
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class PipelineAck implements Writable {
    private long seqno;
    private Status replies[];
    public final static long UNKOWN_SEQNO = -2;

    /** default constructor **/
    public PipelineAck() {
    }
    
    /**
     * Constructor
     * @param seqno sequence number
     * @param replies an array of replies
     */
    public PipelineAck(long seqno, Status[] replies) {
      this.seqno = seqno;
      this.replies = replies;
    }
    
    /**
     * Get the sequence number
     * @return the sequence number
     */
    public long getSeqno() {
      return seqno;
    }
    
    /**
     * Get the number of replies
     * @return the number of replies
     */
    public short getNumOfReplies() {
      return (short)replies.length;
    }
    
    /**
     * get the ith reply
     * @return the the ith reply
     */
    public Status getReply(int i) {
      if (i<0 || i>=replies.length) {
        throw new IllegalArgumentException("The input parameter " + i + 
            " should in the range of [0, " + replies.length);
      }
      return replies[i];
    }
    
    /**
     * Check if this ack contains error status
     * @return true if all statuses are SUCCESS
     */
    public boolean isSuccess() {
      for (Status reply : replies) {
        if (reply != Status.SUCCESS) {
          return false;
        }
      }
      return true;
    }
    
    /**** Writable interface ****/
    @Override // Writable
    public void readFields(DataInput in) throws IOException {
      seqno = in.readLong();
      short numOfReplies = in.readShort();
      replies = new Status[numOfReplies];
      for (int i=0; i<numOfReplies; i++) {
        replies[i] = Status.read(in);
      }
    }

    @Override // Writable
    public void write(DataOutput out) throws IOException {
      //WritableUtils.writeVLong(out, seqno);
      out.writeLong(seqno);
      out.writeShort((short)replies.length);
      for(Status reply : replies) {
        reply.write(out);
      }
    }
    
    @Override //Object
    public String toString() {
      StringBuilder ack = new StringBuilder("Replies for seqno ");
      ack.append( seqno ).append( " are" );
      for(Status reply : replies) {
        ack.append(" ");
        ack.append(reply);
      }
      return ack.toString();
    }
  }

  /**
   * Header data for each packet that goes through the read/write pipelines.
   */
  public static class PacketHeader implements Writable {
    /** Header size for a packet */
    public static final int PKT_HEADER_LEN = ( 4 + /* Packet payload length */
                                               8 + /* offset in block */
                                               8 + /* seqno */
                                               1 + /* isLastPacketInBlock */
                                               4   /* data length */ );

    private int packetLen;
    private long offsetInBlock;
    private long seqno;
    private boolean lastPacketInBlock;
    private int dataLen;

    public PacketHeader() {
    }

    public PacketHeader(int packetLen, long offsetInBlock, long seqno,
                        boolean lastPacketInBlock, int dataLen) {
      this.packetLen = packetLen;
      this.offsetInBlock = offsetInBlock;
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.dataLen = dataLen;
    }

    public int getDataLen() {
      return dataLen;
    }

    public boolean isLastPacketInBlock() {
      return lastPacketInBlock;
    }

    public long getSeqno() {
      return seqno;
    }

    public long getOffsetInBlock() {
      return offsetInBlock;
    }

    public int getPacketLen() {
      return packetLen;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PacketHeader(")
        .append("packetLen=").append(packetLen)
        .append(" offsetInBlock=").append(offsetInBlock)
        .append(" seqno=").append(seqno)
        .append(" lastPacketInBlock=").append(lastPacketInBlock)
        .append(" dataLen=").append(dataLen)
        .append(")");
      return sb.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // Note that it's important for packetLen to come first and not
      // change format -
      // this is used by BlockReceiver to read entire packets with
      // a single read call.
      packetLen = in.readInt();
      offsetInBlock = in.readLong();
      seqno = in.readLong();
      lastPacketInBlock = in.readBoolean();
      dataLen = in.readInt();
    }

    public void readFields(ByteBuffer buf) throws IOException {
      packetLen = buf.getInt();
      offsetInBlock = buf.getLong();
      seqno = buf.getLong();
      lastPacketInBlock = (buf.get() != 0);
      dataLen = buf.getInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(packetLen);
      out.writeLong(offsetInBlock);
      out.writeLong(seqno);
      out.writeBoolean(lastPacketInBlock);
      out.writeInt(dataLen);
    }

    /**
     * Write the header into the buffer.
     * This requires that PKT_HEADER_LEN bytes are available.
     */
    public void putInBuffer(ByteBuffer buf) {
      buf.putInt(packetLen)
        .putLong(offsetInBlock)
        .putLong(seqno)
        .put((byte)(lastPacketInBlock ? 1 : 0))
        .putInt(dataLen);
    }

    /**
     * Perform a sanity check on the packet, returning true if it is sane.
     * @param lastSeqNo the previous sequence number received - we expect the current
     * sequence number to be larger by 1.
     */
    public boolean sanityCheck(long lastSeqNo) {
      // We should only have a non-positive data length for the last packet
      if (dataLen <= 0 && lastPacketInBlock) return false;
      // The last packet should not contain data
      if (lastPacketInBlock && dataLen != 0) return false;
      // Seqnos should always increase by 1 with each packet received
      if (seqno != lastSeqNo + 1) return false;
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof PacketHeader)) return false;
      PacketHeader other = (PacketHeader)o;
      return (other.packetLen == packetLen &&
              other.offsetInBlock == offsetInBlock &&
              other.seqno == seqno &&
              other.lastPacketInBlock == lastPacketInBlock &&
              other.dataLen == dataLen);
    }

    @Override
    public int hashCode() {
      return (int)seqno;
    }
  }

  /**
   * The setting of replace-datanode-on-failure feature.
   */
  public enum ReplaceDatanodeOnFailure {
    /** The feature is disabled in the entire site. */
    DISABLE,
    /** Never add a new datanode. */
    NEVER,
    /**
     * DEFAULT policy:
     *   Let r be the replication number.
     *   Let n be the number of existing datanodes.
     *   Add a new datanode only if r >= 3 and either
     *   (1) floor(r/2) >= n; or
     *   (2) r > n and the block is hflushed/appended.
     */
    DEFAULT,
    /** Always add a new datanode when an existing datanode is removed. */
    ALWAYS;

    /** Check if the feature is enabled. */
    public void checkEnabled() {
      if (this == DISABLE) {
        throw new UnsupportedOperationException(
            "This feature is disabled.  Please refer to "
            + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY
            + " configuration property.");
      }
    }

    /** Is the policy satisfied? */
    public boolean satisfy(
        final short replication, final DatanodeInfo[] existings,
        final boolean isAppend, final boolean isHflushed) {
      final int n = existings == null? 0: existings.length;
      if (n == 0 || n >= replication) {
        //don't need to add datanode for any policy.
        return false;
      } else if (this == DISABLE || this == NEVER) {
        return false;
      } else if (this == ALWAYS) {
        return true;
      } else {
        //DEFAULT
        if (replication < 3) {
          return false;
        } else {
          if (n <= (replication/2)) {
            return true;
          } else {
            return isAppend || isHflushed;
          }
        }
      }
    }

    /** Get the setting from configuration. */
    public static ReplaceDatanodeOnFailure get(final Configuration conf) {
      final boolean enabled = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT);
      if (!enabled) {
        return DISABLE;
      }

      final String policy = conf.get(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY,
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT);
      for(int i = 1; i < values().length; i++) {
        final ReplaceDatanodeOnFailure rdof = values()[i];
        if (rdof.name().equalsIgnoreCase(policy)) {
          return rdof;
        }
      }
      throw new HadoopIllegalArgumentException("Illegal configuration value for "
          + DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY
          + ": " + policy);
    }

    /** Write the setting to configuration. */
    public void write(final Configuration conf) {
      conf.setBoolean(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY,
          this != DISABLE);
      conf.set(
          DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY,
          name());
    }
  }
}
