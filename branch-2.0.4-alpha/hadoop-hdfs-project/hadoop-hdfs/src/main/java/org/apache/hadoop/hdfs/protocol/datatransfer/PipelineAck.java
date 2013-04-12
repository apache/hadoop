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

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;

import com.google.protobuf.TextFormat;

/** Pipeline Acknowledgment **/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PipelineAck {
  PipelineAckProto proto;
  public final static long UNKOWN_SEQNO = -2;

  /** default constructor **/
  public PipelineAck() {
  }
  
  /**
   * Constructor assuming no next DN in pipeline
   * @param seqno sequence number
   * @param replies an array of replies
   */
  public PipelineAck(long seqno, Status[] replies) {
    this(seqno, replies, 0L);
  }

  /**
   * Constructor
   * @param seqno sequence number
   * @param replies an array of replies
   * @param downstreamAckTimeNanos ack RTT in nanoseconds, 0 if no next DN in pipeline
   */
  public PipelineAck(long seqno, Status[] replies, long downstreamAckTimeNanos) {
    proto = PipelineAckProto.newBuilder()
      .setSeqno(seqno)
      .addAllStatus(Arrays.asList(replies))
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
    return (short)proto.getStatusCount();
  }
  
  /**
   * get the ith reply
   * @return the the ith reply
   */
  public Status getReply(int i) {
    return proto.getStatus(i);
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
    for (DataTransferProtos.Status reply : proto.getStatusList()) {
      if (reply != DataTransferProtos.Status.SUCCESS) {
        return false;
      }
    }
    return true;
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
}
