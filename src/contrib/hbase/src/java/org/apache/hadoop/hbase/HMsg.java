/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*******************************************************************************
 * HMsg is for communicating instructions between the HMaster and the 
 * HRegionServers.
 ******************************************************************************/
public class HMsg implements Writable {
  
  // Messages sent from master to region server
  
  /** Start serving the specified region */
  public static final byte MSG_REGION_OPEN = 1;
  
  /** Stop serving the specified region */
  public static final byte MSG_REGION_CLOSE = 2;

  /** Region server is unknown to master. Restart */
  public static final byte MSG_CALL_SERVER_STARTUP = 4;
  
  /** Master tells region server to stop */
  public static final byte MSG_REGIONSERVER_STOP = 5;

  /** Stop serving the specified region and don't report back that it's closed */
  public static final byte MSG_REGION_CLOSE_WITHOUT_REPORT = 6;
  
  /** Stop serving user regions */
  public static final byte MSG_REGIONSERVER_QUIESCE = 7;

  // Messages sent from the region server to the master
  
  /** region server is now serving the specified region */
  public static final byte MSG_REPORT_OPEN = 100;
  
  /** region server is no longer serving the specified region */
  public static final byte MSG_REPORT_CLOSE = 101;
  
  /** region server is processing open request */
  public static final byte MSG_REPORT_PROCESS_OPEN = 102;

  /**
   * region server split the region associated with this message.
   * 
   * note that this message is immediately followed by two MSG_REPORT_OPEN
   * messages, one for each of the new regions resulting from the split
   */
  public static final byte MSG_REPORT_SPLIT = 103;
  
  /**
   * region server is shutting down
   * 
   * note that this message is followed by MSG_REPORT_CLOSE messages for each
   * region the region server was serving, unless it was told to quiesce.
   */
  public static final byte MSG_REPORT_EXITING = 104;
  
  /** region server has closed all user regions but is still serving meta regions */
  public static final byte MSG_REPORT_QUIESCED = 105;

  byte msg;
  HRegionInfo info;

  /** Default constructor. Used during deserialization */
  public HMsg() {
    this.info = new HRegionInfo();
  }

  /**
   * Construct a message with an empty HRegionInfo
   * 
   * @param msg - message code
   */
  public HMsg(byte msg) {
    this.msg = msg;
    this.info = new HRegionInfo();
  }
  
  /**
   * Construct a message with the specified message code and HRegionInfo
   * 
   * @param msg - message code
   * @param info - HRegionInfo
   */
  public HMsg(byte msg, HRegionInfo info) {
    this.msg = msg;
    this.info = info;
  }

  /**
   * Accessor
   * @return message code
   */
  public byte getMsg() {
    return msg;
  }

  /**
   * Accessor
   * @return HRegionInfo
   */
  public HRegionInfo getRegionInfo() {
    return info;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder message = new StringBuilder();
    switch(msg) {
    case MSG_REGION_OPEN:
      message.append("MSG_REGION_OPEN : ");
      break;
      
    case MSG_REGION_CLOSE:
      message.append("MSG_REGION_CLOSE : ");
      break;
      
    case MSG_CALL_SERVER_STARTUP:
      message.append("MSG_CALL_SERVER_STARTUP : ");
      break;
      
    case MSG_REGIONSERVER_STOP:
      message.append("MSG_REGIONSERVER_STOP : ");
      break;
      
    case MSG_REGION_CLOSE_WITHOUT_REPORT:
      message.append("MSG_REGION_CLOSE_WITHOUT_REPORT : ");
      break;
      
    case MSG_REGIONSERVER_QUIESCE:
      message.append("MSG_REGIONSERVER_QUIESCE : ");
      break;
      
    case MSG_REPORT_PROCESS_OPEN:
      message.append("MSG_REPORT_PROCESS_OPEN : ");
      break;
      
    case MSG_REPORT_OPEN:
      message.append("MSG_REPORT_OPEN : ");
      break;
      
    case MSG_REPORT_CLOSE:
      message.append("MSG_REPORT_CLOSE : ");
      break;
      
    case MSG_REPORT_SPLIT:
      message.append("MSG_REGION_SPLIT : ");
      break;
      
    case MSG_REPORT_EXITING:
      message.append("MSG_REPORT_EXITING : ");
      break;
      
    case MSG_REPORT_QUIESCED:
      message.append("MSG_REPORT_QUIESCED : ");
      break;
      
    default:
      message.append("unknown message code (");
      message.append(msg);
      message.append(") : ");
      break;
    }
    message.append(info == null ? "null" : info.getRegionName());
    return message.toString();
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
     out.writeByte(msg);
     info.write(out);
   }

  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
     this.msg = in.readByte();
     this.info.readFields(in);
   }
}
