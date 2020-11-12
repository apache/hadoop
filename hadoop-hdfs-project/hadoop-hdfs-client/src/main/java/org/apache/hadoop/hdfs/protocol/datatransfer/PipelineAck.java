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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.hdfs.util.LongBitFormat;

/** Pipeline Acknowledgment **/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PipelineAck {
  PipelineAckProto proto;
  public final static long UNKOWN_SEQNO = -2;
  final static int OOB_START = Status.OOB_RESTART_VALUE; // the first OOB type
  final static int OOB_END = Status.OOB_RESERVED3_VALUE; // the last OOB type

  public enum ECN {
    DISABLED(0),
    SUPPORTED(1),
    SUPPORTED2(2),
    CONGESTED(3);

    private final int value;
    private static final ECN[] VALUES = values();
    static ECN valueOf(int value) {
      return VALUES[value];
    }

    ECN(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  private enum StatusFormat {
    STATUS(null, 4),
    RESERVED(STATUS.BITS, 1),
    ECN_BITS(RESERVED.BITS, 2);

    private final LongBitFormat BITS;

    StatusFormat(LongBitFormat prev, int bits) {
      BITS = new LongBitFormat(name(), prev, bits, 0);
    }

    static Status getStatus(int header) {
      return Status.forNumber((int) STATUS.BITS.retrieve(header));
    }

    static ECN getECN(int header) {
      return ECN.valueOf((int) ECN_BITS.BITS.retrieve(header));
    }

    public static int setStatus(int old, Status status) {
      return (int) STATUS.BITS.combine(status.getNumber(), old);
    }

    public static int setECN(int old, ECN ecn) {
      return (int) ECN_BITS.BITS.combine(ecn.getValue(), old);
    }
  }

  /** default constructor **/
  public PipelineAck() {
  }

  /**
   * Constructor assuming no next DN in pipeline
   * @param seqno sequence number
   * @param replies an array of replies
   */
  public PipelineAck(long seqno, int[] replies) {
    this(seqno, replies, 0L);
  }

  /**
   * Constructor
   * @param seqno sequence number
   * @param replies an array of replies
   * @param downstreamAckTimeNanos ack RTT in nanoseconds, 0 if no next DN in pipeline
   */
  public PipelineAck(long seqno, int[] replies,
                     long downstreamAckTimeNanos) {
    ArrayList<Status> statusList = Lists.newArrayList();
    ArrayList<Integer> flagList = Lists.newArrayList();
    for (int r : replies) {
      statusList.add(StatusFormat.getStatus(r));
      flagList.add(r);
    }
    proto = PipelineAckProto.newBuilder()
      .setSeqno(seqno)
      .addAllReply(statusList)
      .addAllFlag(flagList)
      .setDownstreamAckTimeNanos(downstreamAckTimeNanos)
      .build();
  }

  /**
   * Get the sequence number
   * @return the sequence number
   */
  public long getSeqno() {
    return proto.getSeqno();
  }

  /**
   * Get the number of replies
   * @return the number of replies
   */
  public short getNumOfReplies() {
    return (short)proto.getReplyCount();
  }

  /**
   * get the header flag of ith reply
   */
  public int getHeaderFlag(int i) {
    if (proto.getFlagCount() > 0) {
      return proto.getFlag(i);
    } else {
      return combineHeader(ECN.DISABLED, proto.getReply(i));
    }
  }

  public int getFlag(int i) {
    return proto.getFlag(i);
  }

  /**
   * Get the time elapsed for downstream ack RTT in nanoseconds
   * @return time elapsed for downstream ack in nanoseconds, 0 if no next DN in pipeline
   */
  public long getDownstreamAckTimeNanos() {
    return proto.getDownstreamAckTimeNanos();
  }

  /**
   * Check if this ack contains error status
   * @return true if all statuses are SUCCESS
   */
  public boolean isSuccess() {
    for (Status s : proto.getReplyList()) {
      if (s != Status.SUCCESS) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the OOB status if this ack contains one.
   * @return null if it is not an OOB ack.
   */
  public Status getOOBStatus() {
    // Normal data transfer acks will have a valid sequence number, so
    // this will return right away in most cases.
    if (getSeqno() != UNKOWN_SEQNO) {
      return null;
    }
    for (Status s : proto.getReplyList()) {
      // The following check is valid because protobuf guarantees to
      // preserve the ordering of enum elements.
      if (s.getNumber() >= OOB_START && s.getNumber() <= OOB_END) {
        return s;
      }
    }
    return null;
  }

  /** Get the Restart OOB ack status */
  public static Status getRestartOOBStatus() {
    return Status.OOB_RESTART;
  }

  /** return true if it is the restart OOB status code  */
  public static boolean isRestartOOBStatus(Status st) {
    return st.equals(Status.OOB_RESTART);
  }

  /**** Writable interface ****/
  public void readFields(InputStream in) throws IOException {
    proto = PipelineAckProto.parseFrom(vintPrefixed(in));
  }

  public void write(OutputStream out) throws IOException {
    proto.writeDelimitedTo(out);
  }

  @Override //Object
  public String toString() {
    return TextFormat.shortDebugString(proto);
  }

  public static Status getStatusFromHeader(int header) {
    return StatusFormat.getStatus(header);
  }

  public static ECN getECNFromHeader(int header) {
    return StatusFormat.getECN(header);
  }

  public static int setStatusForHeader(int old, Status status) {
    return StatusFormat.setStatus(old, status);
  }

  public static int combineHeader(ECN ecn, Status status) {
    int header = 0;
    header = StatusFormat.setStatus(header, status);
    header = StatusFormat.setECN(header, ecn);
    return header;
  }
}
