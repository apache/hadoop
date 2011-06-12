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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Transfer data to/from datanode using a streaming protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTransferProtocol {
  
  
  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious. 
   */
  /*
   * Version 19:
   *    Change the block packet ack protocol to include seqno,
   *    numberOfReplies, reply0, reply1, ...
   */
  public static final int DATA_TRANSFER_VERSION = 19;

  /** Operation */
  public enum Op {
    WRITE_BLOCK((byte)80),
    READ_BLOCK((byte)81),
    READ_METADATA((byte)82),
    REPLACE_BLOCK((byte)83),
    COPY_BLOCK((byte)84),
    BLOCK_CHECKSUM((byte)85);

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
    PIPELINE_SETUP_CREATE;
    
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
    public static void opReadBlock(DataOutputStream out,
        long blockId, long blockGs, long blockOffset, long blockLen,
        String clientName, BlockAccessToken accessToken) throws IOException {
      op(out, Op.READ_BLOCK);

      out.writeLong(blockId);
      out.writeLong(blockGs);
      out.writeLong(blockOffset);
      out.writeLong(blockLen);
      Text.writeString(out, clientName);
      accessToken.write(out);
      out.flush();
    }
    
    /** Send OP_WRITE_BLOCK */
    public static void opWriteBlock(DataOutputStream out,
        long blockId, long blockGs, int pipelineSize, 
        BlockConstructionStage stage, long newGs, long minBytesRcvd,
        long maxBytesRcvd, String client, DatanodeInfo src, 
        DatanodeInfo[] targets, BlockAccessToken accesstoken) throws IOException {
      op(out, Op.WRITE_BLOCK);

      out.writeLong(blockId);
      out.writeLong(blockGs);
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
      out.writeInt(targets.length - 1);
      for (int i = 1; i < targets.length; i++) {
        targets[i].write(out);
      }

      accesstoken.write(out);
    }
    
    /** Send OP_REPLACE_BLOCK */
    public static void opReplaceBlock(DataOutputStream out,
        long blockId, long blockGs, String storageId, DatanodeInfo src,
        BlockAccessToken accesstoken) throws IOException {
      op(out, Op.REPLACE_BLOCK);

      out.writeLong(blockId);
      out.writeLong(blockGs);
      Text.writeString(out, storageId);
      src.write(out);
      accesstoken.write(out);
      out.flush();
    }

    /** Send OP_COPY_BLOCK */
    public static void opCopyBlock(DataOutputStream out,
        long blockId, long blockGs, BlockAccessToken accesstoken) throws IOException {
      op(out, Op.COPY_BLOCK);

      out.writeLong(blockId);
      out.writeLong(blockGs);
      accesstoken.write(out);
      out.flush();
    }

    /** Send OP_BLOCK_CHECKSUM */
    public static void opBlockChecksum(DataOutputStream out,
        long blockId, long blockGs, BlockAccessToken accesstoken) throws IOException {
      op(out, Op.BLOCK_CHECKSUM);

      out.writeLong(blockId);
      out.writeLong(blockGs);
      accesstoken.write(out);
      out.flush();
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
      default:
        throw new IOException("Unknown op " + op + " in data stream");
      }
    }

    /** Receive OP_READ_BLOCK */
    private void opReadBlock(DataInputStream in) throws IOException {
      final long blockId = in.readLong();          
      final long blockGs = in.readLong();
      final long offset = in.readLong();
      final long length = in.readLong();
      final String client = Text.readString(in);
      final BlockAccessToken accesstoken = readAccessToken(in);

      opReadBlock(in, blockId, blockGs, offset, length, client, accesstoken);
    }

    /**
     * Abstract OP_READ_BLOCK method.
     * Read a block.
     */
    protected abstract void opReadBlock(DataInputStream in,
        long blockId, long blockGs, long offset, long length,
        String client, BlockAccessToken accesstoken) throws IOException;
    
    /** Receive OP_WRITE_BLOCK */
    private void opWriteBlock(DataInputStream in) throws IOException {
      final long blockId = in.readLong();          
      final long blockGs = in.readLong();
      final int pipelineSize = in.readInt(); // num of datanodes in entire pipeline
      final BlockConstructionStage stage = 
        BlockConstructionStage.readFields(in);
      final long newGs = WritableUtils.readVLong(in);
      final long minBytesRcvd = WritableUtils.readVLong(in);
      final long maxBytesRcvd = WritableUtils.readVLong(in);
      final String client = Text.readString(in); // working on behalf of this client
      final DatanodeInfo src = in.readBoolean()? DatanodeInfo.read(in): null;

      final int nTargets = in.readInt();
      if (nTargets < 0) {
        throw new IOException("Mislabelled incoming datastream.");
      }
      final DatanodeInfo targets[] = new DatanodeInfo[nTargets];
      for (int i = 0; i < targets.length; i++) {
        targets[i] = DatanodeInfo.read(in);
      }
      final BlockAccessToken accesstoken = readAccessToken(in);

      opWriteBlock(in, blockId, blockGs, pipelineSize, stage,
          newGs, minBytesRcvd, maxBytesRcvd, client, src, targets, accesstoken);
    }

    /**
     * Abstract OP_WRITE_BLOCK method. 
     * Write a block.
     */
    protected abstract void opWriteBlock(DataInputStream in,
        long blockId, long blockGs,
        int pipelineSize, BlockConstructionStage stage,
        long newGs, long minBytesRcvd, long maxBytesRcvd,
        String client, DatanodeInfo src, DatanodeInfo[] targets,
        BlockAccessToken accesstoken) throws IOException;

    /** Receive OP_REPLACE_BLOCK */
    private void opReplaceBlock(DataInputStream in) throws IOException {
      final long blockId = in.readLong();          
      final long blockGs = in.readLong();
      final String sourceId = Text.readString(in); // read del hint
      final DatanodeInfo src = DatanodeInfo.read(in); // read proxy source
      final BlockAccessToken accesstoken = readAccessToken(in);

      opReplaceBlock(in, blockId, blockGs, sourceId, src, accesstoken);
    }

    /**
     * Abstract OP_REPLACE_BLOCK method.
     * It is used for balancing purpose; send to a destination
     */
    protected abstract void opReplaceBlock(DataInputStream in,
        long blockId, long blockGs, String sourceId, DatanodeInfo src,
        BlockAccessToken accesstoken) throws IOException;

    /** Receive OP_COPY_BLOCK */
    private void opCopyBlock(DataInputStream in) throws IOException {
      final long blockId = in.readLong();          
      final long blockGs = in.readLong();
      final BlockAccessToken accesstoken = readAccessToken(in);

      opCopyBlock(in, blockId, blockGs, accesstoken);
    }

    /**
     * Abstract OP_COPY_BLOCK method.
     * It is used for balancing purpose; send to a proxy source.
     */
    protected abstract void opCopyBlock(DataInputStream in,
        long blockId, long blockGs, BlockAccessToken accesstoken) throws IOException;

    /** Receive OP_BLOCK_CHECKSUM */
    private void opBlockChecksum(DataInputStream in) throws IOException {
      final long blockId = in.readLong();          
      final long blockGs = in.readLong();
      final BlockAccessToken accesstoken = readAccessToken(in);

      opBlockChecksum(in, blockId, blockGs, accesstoken);
    }

    /**
     * Abstract OP_BLOCK_CHECKSUM method.
     * Get the checksum of a block 
     */
    protected abstract void opBlockChecksum(DataInputStream in,
        long blockId, long blockGs, BlockAccessToken accesstoken) throws IOException;

    /** Read an AccessToken */
    static private BlockAccessToken readAccessToken(DataInputStream in
        ) throws IOException {
      final BlockAccessToken t = new BlockAccessToken();
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
}
