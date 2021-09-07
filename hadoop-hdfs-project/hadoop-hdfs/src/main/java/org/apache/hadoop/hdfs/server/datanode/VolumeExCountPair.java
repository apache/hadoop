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
package org.apache.hadoop.hdfs.server.datanode;

/**
 * Record volume's IOException total counts and update timestamp.
 */
public class VolumeExCountPair {

  private long prevTs;
  private long ioExceptionCnt;

  public VolumeExCountPair() {
  }

  public VolumeExCountPair(long prevTimeStamp, long ioExceptionCnt) {
    this.prevTs = prevTimeStamp;
    this.ioExceptionCnt = ioExceptionCnt;
  }

  public void setNewPair(long now, long curExCnt) {
    setPrevTs(now);
    setIoExceptionCnt(curExCnt);
  }

  public VolumeExCountPair getPair() {
    return new VolumeExCountPair(prevTs, ioExceptionCnt);
  }

  public void setPrevTs(long prevTs) {
    this.prevTs = prevTs;
  }

  public void setIoExceptionCnt(long ioExceptionCnt) {
    this.ioExceptionCnt = ioExceptionCnt;
  }

  public long getPrevTs() {
    return prevTs;
  }

  public long getIoExceptionCnt() {
    return ioExceptionCnt;
  }
}