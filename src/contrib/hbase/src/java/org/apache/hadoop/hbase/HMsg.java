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
 * HMsg is for communicating instructions between the HMaster and the 
 * HRegionServers.
 ******************************************************************************/
public class HMsg implements Writable {
  public static final byte MSG_REGION_OPEN = 1;
  public static final byte MSG_REGION_CLOSE = 2;
  public static final byte MSG_REGION_MERGE = 3;
  public static final byte MSG_CALL_SERVER_STARTUP = 4;
  public static final byte MSG_REGIONSERVER_STOP = 5;
  public static final byte MSG_REGION_CLOSE_WITHOUT_REPORT = 6;
  public static final byte MSG_REGION_CLOSE_AND_DELETE = 7;

  public static final byte MSG_REPORT_OPEN = 100;
  public static final byte MSG_REPORT_CLOSE = 101;
  public static final byte MSG_REGION_SPLIT = 102;
  public static final byte MSG_NEW_REGION = 103;

  byte msg;
  HRegionInfo info;

  public HMsg() {
    this.info = new HRegionInfo();
  }

  public HMsg(byte msg) {
    this.msg = msg;
    this.info = new HRegionInfo();
  }
  
  public HMsg(byte msg, HRegionInfo info) {
    this.msg = msg;
    this.info = info;
  }

  public byte getMsg() {
    return msg;
  }

  public HRegionInfo getRegionInfo() {
    return info;
  }


  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

   public void write(DataOutput out) throws IOException {
     out.writeByte(msg);
     info.write(out);
   }

   public void readFields(DataInput in) throws IOException {
     this.msg = in.readByte();
     this.info.readFields(in);
   }
}
