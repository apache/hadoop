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
 * @author Mike Cafarella
 ************************************/
public interface FSConstants {
    public static int MIN_BLOCKS_FOR_WRITE = 5;

    public static final long WRITE_COMPLETE = 0xcafae11a;

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
    public static final byte OP_READSKIP_BLOCK = (byte) 82;
    public static final byte OP_READ_RANGE_BLOCK = (byte) 83;

    // Encoding types
    public static final byte RUNLENGTH_ENCODING = 0;
    public static final byte CHUNKED_ENCODING = 1;

    // Return codes for file create
    public static final int OPERATION_FAILED = 0;
    public static final int STILL_WAITING = 1;
    public static final int COMPLETE_SUCCESS = 2;

    //
    // Timeouts, constants
    //
    public static long HEARTBEAT_INTERVAL = 3;
    public static long BLOCKREPORT_INTERVAL = 60 * 60 * 1000;
    public static final long LEASE_SOFTLIMIT_PERIOD = 60 * 1000;
    public static final long LEASE_HARDLIMIT_PERIOD = 60 * LEASE_SOFTLIMIT_PERIOD;
    public static int READ_TIMEOUT = 60 * 1000;

    // We need to limit the length and depth of a path in the filesystem.  HADOOP-438
    // Currently we set the maximum length to 8k characters and the maximum depth to 1k.  
    public static int MAX_PATH_LENGTH = 8000;
    public static int MAX_PATH_DEPTH = 1000;
    
    //TODO mb@media-style.com: should be conf injected?
    public static final int BUFFER_SIZE = new Configuration().getInt("io.file.buffer.size", 4096);

    // SafeMode actions
    public enum SafeModeAction{ SAFEMODE_LEAVE, SAFEMODE_ENTER, SAFEMODE_GET; }

    // Version is reflected in the dfs image and edit log files.
    // Version is reflected in the data storage file.
    // Versions are negative.
    // Decrement DFS_CURRENT_VERSION to define a new version.
    public static final int DFS_CURRENT_VERSION = -3;
    // Current version: New operations OP_DATANODE_REMOVE and OP_DATANODE_ADD
    // are introduced
}
