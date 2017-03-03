/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.server.datanode;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

/**
 * Keeps track of how much work has finished.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class DiskBalancerWorkItem {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(DiskBalancerWorkItem.class);

  private  long startTime;
  private long secondsElapsed;
  private long bytesToCopy;
  private long bytesCopied;
  private long errorCount;
  private String errMsg;
  private long blocksCopied;

  private long maxDiskErrors;
  private long tolerancePercent;
  private long bandwidth;

  /**
   * Empty constructor for Json serialization.
   */
  public DiskBalancerWorkItem() {

  }


  /**
   * Constructs a DiskBalancerWorkItem.
   *
   * @param bytesToCopy - Total bytes to copy from a disk
   * @param bytesCopied - Copied So far.
   */
  public DiskBalancerWorkItem(long bytesToCopy, long bytesCopied) {
    this.bytesToCopy = bytesToCopy;
    this.bytesCopied = bytesCopied;
  }

  /**
   * Reads a DiskBalancerWorkItem Object from a Json String.
   *
   * @param json - Json String.
   * @return DiskBalancerWorkItem Object
   * @throws IOException
   */
  public static DiskBalancerWorkItem parseJson(String json) throws IOException {
    Preconditions.checkNotNull(json);
    return READER.readValue(json);
  }

  /**
   * Gets the error message.
   */
  public String getErrMsg() {
    return errMsg;
  }

  /**
   * Sets the error message.
   *
   * @param errMsg - Msg.
   */
  public void setErrMsg(String errMsg) {
    this.errMsg = errMsg;
  }

  /**
   * Returns the number of errors encountered.
   *
   * @return long
   */
  public long getErrorCount() {
    return errorCount;
  }

  /**
   * Incs Error Count.
   */
  public void incErrorCount() {
    this.errorCount++;
  }

  /**
   * Returns bytes copied so far.
   *
   * @return long
   */
  public long getBytesCopied() {
    return bytesCopied;
  }

  /**
   * Sets bytes copied so far.
   *
   * @param bytesCopied - long
   */
  public void setBytesCopied(long bytesCopied) {
    this.bytesCopied = bytesCopied;
  }

  /**
   * Increments bytesCopied by delta.
   *
   * @param delta - long
   */
  public void incCopiedSoFar(long delta) {
    this.bytesCopied += delta;
  }

  /**
   * Returns bytes to copy.
   *
   * @return - long
   */
  public long getBytesToCopy() {
    return bytesToCopy;
  }

  /**
   * Returns number of blocks copied for this DiskBalancerWorkItem.
   *
   * @return long count of blocks.
   */
  public long getBlocksCopied() {
    return blocksCopied;
  }

  /**
   * increments the number of blocks copied.
   */
  public void incBlocksCopied() {
    blocksCopied++;
  }

  /**
   * returns a serialized json string.
   *
   * @return String - json
   * @throws IOException
   */
  public String toJson() throws IOException {
    return MAPPER.writeValueAsString(this);
  }

  /**
   * Sets the Error counts for this step.
   *
   * @param errorCount long.
   */
  public void setErrorCount(long errorCount) {
    this.errorCount = errorCount;
  }

  /**
   * Number of blocks copied so far.
   *
   * @param blocksCopied Blocks copied.
   */
  public void setBlocksCopied(long blocksCopied) {
    this.blocksCopied = blocksCopied;
  }

  /**
   * Gets maximum disk errors to tolerate before we fail this copy step.
   *
   * @return long.
   */
  public long getMaxDiskErrors() {
    return maxDiskErrors;
  }

  /**
   * Sets maximum disk errors to tolerate before we fail this copy step.
   *
   * @param maxDiskErrors long
   */
  public void setMaxDiskErrors(long maxDiskErrors) {
    this.maxDiskErrors = maxDiskErrors;
  }

  /**
   * Allowed deviation from ideal storage in percentage.
   *
   * @return long
   */
  public long getTolerancePercent() {
    return tolerancePercent;
  }

  /**
   * Sets the tolerance percentage.
   *
   * @param tolerancePercent - tolerance.
   */
  public void setTolerancePercent(long tolerancePercent) {
    this.tolerancePercent = tolerancePercent;
  }

  /**
   * Max disk bandwidth to use. MB per second.
   *
   * @return - long.
   */
  public long getBandwidth() {
    return bandwidth;
  }

  /**
   * Sets max disk bandwidth to use, in MBs per second.
   *
   * @param bandwidth - long.
   */
  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }


  /**
   * Records the Start time of execution.
   * @return startTime
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Sets the Start time.
   * @param startTime  - Time stamp for start of execution.
   */
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Gets the number of seconds elapsed from the start time.
   *
   * The reason why we have this is of time skews. The client's current time
   * may not match with the server time stamp, hence the elapsed second
   * cannot be computed from only startTime.
   *
   * @return seconds elapsed from start time.
   */
  public long getSecondsElapsed() {
    return secondsElapsed;
  }

  /**
   * Sets number of seconds elapsed.
   *
   * This is updated whenever we update the other counters.
   * @param secondsElapsed  - seconds elapsed.
   */
  public void setSecondsElapsed(long secondsElapsed) {
    this.secondsElapsed = secondsElapsed;
  }
}