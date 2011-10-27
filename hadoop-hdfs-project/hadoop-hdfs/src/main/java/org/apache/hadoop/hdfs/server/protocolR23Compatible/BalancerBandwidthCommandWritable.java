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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Balancer bandwidth command instructs each datanode to change its value for
 * the max amount of network bandwidth it may use during the block balancing
 * operation.
 * 
 * The Balancer Bandwidth Command contains the new bandwidth value as its
 * payload. The bandwidth value is in bytes per second.
 */
public class BalancerBandwidthCommandWritable extends DatanodeCommandWritable {
  private final static long BBC_DEFAULTBANDWIDTH = 0L;

  private long bandwidth;

  /**
   * Balancer Bandwidth Command constructor. Sets bandwidth to 0.
   */
  BalancerBandwidthCommandWritable() {
    this(BBC_DEFAULTBANDWIDTH);
  }

  /**
   * Balancer Bandwidth Command constructor.
   * @param bandwidth Blanacer bandwidth in bytes per second.
   */
  public BalancerBandwidthCommandWritable(long bandwidth) {
    super(DatanodeWireProtocol.DNA_BALANCERBANDWIDTHUPDATE);
    this.bandwidth = bandwidth;
  }

  /**
   * Get current value of the max balancer bandwidth in bytes per second.
   * @return bandwidth Blanacer bandwidth in bytes per second for this datanode.
   */
  public long getBalancerBandwidthValue() {
    return this.bandwidth;
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(BalancerBandwidthCommandWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new BalancerBandwidthCommandWritable();
          }
        });
  }

  /**
   * Writes the bandwidth payload to the Balancer Bandwidth Command packet.
   * @param out DataOutput stream used for writing commands to the datanode.
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(this.bandwidth);
  }

  /**
   * Reads the bandwidth payload from the Balancer Bandwidth Command packet.
   * @param in DataInput stream used for reading commands to the datanode.
   * @throws IOException
   */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.bandwidth = in.readLong();
  }

  @Override
  public DatanodeCommand convert() {
    return new BalancerBandwidthCommand(bandwidth);
  }

  public static DatanodeCommandWritable convert(BalancerBandwidthCommand cmd) {
    return new BalancerBandwidthCommandWritable(cmd.getBalancerBandwidthValue());
  }
}
