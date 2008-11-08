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
package org.apache.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;

/************************************
 * Some handy constants
 *
 ************************************/
public interface FSConstants {
  public static int MIN_BLOCKS_FOR_WRITE = 5;

  //
  // IPC Opcodes 
  //
  // Processed at namenode
  public static final byte OP_ERROR = (byte) 0;
  public static final byte OP_HEARTBEAT = (byte) 1;
  public static final byte OP_BLOCKRECEIVED = (byte) 2;
  public static final byte OP_BLOCKREPORT = (byte) 3;
  public static final byte OP_TRANSFERDATA = (byte) 4;

  // Processed at namenode, from client
  public static final byte OP_CLIENT_OPEN = (byte) 20;
  public static final byte OP_CLIENT_STARTFILE = (byte) 21;
  public static final byte OP_CLIENT_ADDBLOCK = (byte) 22;
  public static final byte OP_CLIENT_RENAMETO = (byte) 23;
  public static final byte OP_CLIENT_DELETE = (byte) 24;  
  public static final byte OP_CLIENT_COMPLETEFILE = (byte) 25;
  public static final byte OP_CLIENT_LISTING = (byte) 26;
  public static final byte OP_CLIENT_OBTAINLOCK = (byte) 27;
  public static final byte OP_CLIENT_RELEASELOCK = (byte) 28;
  public static final byte OP_CLIENT_EXISTS = (byte) 29;
  public static final byte OP_CLIENT_ISDIR = (byte) 30;
  public static final byte OP_CLIENT_MKDIRS = (byte) 31;
  public static final byte OP_CLIENT_RENEW_LEASE = (byte) 32;
  public static final byte OP_CLIENT_ABANDONBLOCK = (byte) 33;
  public static final byte OP_CLIENT_RAWSTATS = (byte) 34;
  public static final byte OP_CLIENT_DATANODEREPORT = (byte) 35;
  public static final byte OP_CLIENT_DATANODE_HINTS = (byte) 36;
    
  // Processed at datanode, back from namenode
  public static final byte OP_ACK = (byte) 40;
  public static final byte OP_TRANSFERBLOCKS = (byte) 41;    
  public static final byte OP_INVALIDATE_BLOCKS = (byte) 42;
  public static final byte OP_FAILURE = (byte) 43;

  // Processed at client, back from namenode
  public static final byte OP_CLIENT_OPEN_ACK = (byte) 60;
  public static final byte OP_CLIENT_STARTFILE_ACK = (byte) 61;
  public static final byte OP_CLIENT_ADDBLOCK_ACK = (byte) 62;
  public static final byte OP_CLIENT_RENAMETO_ACK = (byte) 63;
  public static final byte OP_CLIENT_DELETE_ACK = (byte) 64;
  public static final byte OP_CLIENT_COMPLETEFILE_ACK = (byte) 65;
  public static final byte OP_CLIENT_TRYAGAIN = (byte) 66;
  public static final byte OP_CLIENT_LISTING_ACK = (byte) 67;
  public static final byte OP_CLIENT_OBTAINLOCK_ACK = (byte) 68;
  public static final byte OP_CLIENT_RELEASELOCK_ACK = (byte) 69;
  public static final byte OP_CLIENT_EXISTS_ACK = (byte) 70;  
  public static final byte OP_CLIENT_ISDIR_ACK = (byte) 71;
  public static final byte OP_CLIENT_MKDIRS_ACK = (byte) 72;
  public static final byte OP_CLIENT_RENEW_LEASE_ACK = (byte) 73;    
  public static final byte OP_CLIENT_ABANDONBLOCK_ACK = (byte) 74;
  public static final byte OP_CLIENT_RAWSTATS_ACK = (byte) 75;
  public static final byte OP_CLIENT_DATANODEREPORT_ACK = (byte) 76;
  public static final byte OP_CLIENT_DATANODE_HINTS_ACK = (byte) 77;

  // Processed at datanode stream-handler
  public static final byte OP_WRITE_BLOCK = (byte) 80;
  public static final byte OP_READ_BLOCK = (byte) 81;
  public static final byte OP_READ_METADATA = (byte) 82;
  public static final byte OP_REPLACE_BLOCK = (byte) 83;
  public static final byte OP_COPY_BLOCK = (byte) 84;
  
  public static final int OP_STATUS_SUCCESS = 0;  
  public static final int OP_STATUS_ERROR = 1;  
  public static final int OP_STATUS_ERROR_CHECKSUM = 2;  
  public static final int OP_STATUS_ERROR_INVALID = 3;  
  public static final int OP_STATUS_ERROR_EXISTS = 4;  
  public static final int OP_STATUS_CHECKSUM_OK = 5;  

  
  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious. 
   */
  /*
   * Version 11:
   *    OP_WRITE_BLOCK sends a boolean. If its value is true, an additonal 
   *    DatanodeInfo of client requesting transfer is also sent. 
   */
  public static final int DATA_TRANSFER_VERSION = 11;

  // Return codes for file create
  public static final int OPERATION_FAILED = 0;
  public static final int STILL_WAITING = 1;
  public static final int COMPLETE_SUCCESS = 2;

  // Chunk the block Invalidate message
  public static final int BLOCK_INVALIDATE_CHUNK = 100;

  //
  // Timeouts, constants
  //
  public static long HEARTBEAT_INTERVAL = 3;
  public static long BLOCKREPORT_INTERVAL = 60 * 60 * 1000;
  public static long BLOCKREPORT_INITIAL_DELAY = 0;
  public static final long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
  public static final long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;
  public static final long LEASE_RECOVER_PERIOD = 10 * 1000; //in ms

  public static int READ_TIMEOUT = 60 * 1000;
  public static int WRITE_TIMEOUT = 8 * 60 * 1000;  
  public static int WRITE_TIMEOUT_EXTENSION = 5 * 1000; //for write pipeline

  // We need to limit the length and depth of a path in the filesystem.  HADOOP-438
  // Currently we set the maximum length to 8k characters and the maximum depth to 1k.  
  public static int MAX_PATH_LENGTH = 8000;
  public static int MAX_PATH_DEPTH = 1000;
    
  public static final int BUFFER_SIZE = new Configuration().getInt("io.file.buffer.size", 4096);
  //Used for writing header etc.
  static final int SMALL_BUFFER_SIZE = Math.min(BUFFER_SIZE/2, 512);
  //TODO mb@media-style.com: should be conf injected?
  public static final long DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
  public static final int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;

  public static final int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;

  // SafeMode actions
  public enum SafeModeAction{ SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET; }

  // Startup options
  public enum StartupOption{
    FORMAT  ("-format"),
    REGULAR ("-regular"),
    UPGRADE ("-upgrade"),
    ROLLBACK("-rollback"),
    FINALIZE("-finalize"),
    IMPORT  ("-importCheckpoint");
    
    private String name = null;
    private StartupOption(String arg) {this.name = arg;}
    String getName() {return name;}
  }

  // type of the datanode report
  public static enum DatanodeReportType {ALL, LIVE, DEAD }

  // checkpoint states
  public enum CheckpointStates{ START, ROLLED_EDITS, UPLOAD_START, UPLOAD_DONE; }

  /**
   * Type of the node
   */
  static public enum NodeType {
    NAME_NODE,
    DATA_NODE;
  }

  /**
   * Distributed upgrade actions:
   * 
   * 1. Get upgrade status.
   * 2. Get detailed upgrade status.
   * 3. Proceed with the upgrade if it is stuck, no matter what the status is.
   */
  public static enum UpgradeAction {
    GET_STATUS,
    DETAILED_STATUS,
    FORCE_PROCEED;
  }

  // Version is reflected in the dfs image and edit log files.
  // Version is reflected in the data storage file.
  // Versions are negative.
  // Decrement LAYOUT_VERSION to define a new version.
  public static final int LAYOUT_VERSION = -16;
  // Current version: 
  // Change edit log and fsimage to support quotas
}
