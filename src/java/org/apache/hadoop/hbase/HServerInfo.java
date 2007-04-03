/**
 * Copyright 2006-7 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.hadoop.io.*;

import java.io.*;

/*******************************************************************************
 * HRSInfo contains metainfo about an HRegionServer, including details about the
 * source machine and load statistics.
 ******************************************************************************/
public class HServerInfo implements Writable {
  private HServerAddress serverAddress;
  private long startCode;

  public HServerInfo() {
    this.serverAddress = new HServerAddress();
    this.startCode = 0;
  }
  
  public HServerInfo(HServerAddress serverAddress, long startCode) {
    this.serverAddress = new HServerAddress(serverAddress);
    this.startCode = startCode;
  }
  
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
  }
  
  public HServerAddress getServerAddress() {
    return serverAddress;
  }
  
  public long getStartCode() {
    return startCode;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
  }

  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
  }
}
