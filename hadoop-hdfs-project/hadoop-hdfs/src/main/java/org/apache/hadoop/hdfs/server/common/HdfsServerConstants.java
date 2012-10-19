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
package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext;

/************************************
 * Some handy internal HDFS constants
 *
 ************************************/

@InterfaceAudience.Private
public final class HdfsServerConstants {
  /* Hidden constructor */
  private HdfsServerConstants() { }
  
  /**
   * Type of the node
   */
  static public enum NodeType {
    NAME_NODE,
    DATA_NODE,
    JOURNAL_NODE;
  }

  /** Startup options */
  static public enum StartupOption{
    FORMAT  ("-format"),
    CLUSTERID ("-clusterid"),
    GENCLUSTERID ("-genclusterid"),
    REGULAR ("-regular"),
    BACKUP  ("-backup"),
    CHECKPOINT("-checkpoint"),
    UPGRADE ("-upgrade"),
    ROLLBACK("-rollback"),
    FINALIZE("-finalize"),
    IMPORT  ("-importCheckpoint"),
    BOOTSTRAPSTANDBY("-bootstrapStandby"),
    INITIALIZESHAREDEDITS("-initializeSharedEdits"),
    RECOVER  ("-recover"),
    FORCE("-force"),
    NONINTERACTIVE("-nonInteractive");
    
    private final String name;
    
    // Used only with format and upgrade options
    private String clusterId = null;
    
    // Used only with format option
    private boolean isForceFormat = false;
    private boolean isInteractiveFormat = true;
    
    // Used only with recovery option
    private int force = 0;

    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
    public NamenodeRole toNodeRole() {
      switch(this) {
      case BACKUP: 
        return NamenodeRole.BACKUP;
      case CHECKPOINT: 
        return NamenodeRole.CHECKPOINT;
      default:
        return NamenodeRole.NAMENODE;
      }
    }
    
    public void setClusterId(String cid) {
      clusterId = cid;
    }

    public String getClusterId() {
      return clusterId;
    }

    public MetaRecoveryContext createRecoveryContext() {
      if (!name.equals(RECOVER.name))
        return null;
      return new MetaRecoveryContext(force);
    }

    public void setForce(int force) {
      this.force = force;
    }
    
    public int getForce() {
      return this.force;
    }
    
    public boolean getForceFormat() {
      return isForceFormat;
    }
    
    public void setForceFormat(boolean force) {
      isForceFormat = force;
    }
    
    public boolean getInteractiveFormat() {
      return isInteractiveFormat;
    }
    
    public void setInteractiveFormat(boolean interactive) {
      isInteractiveFormat = interactive;
    }
  }

  // Timeouts for communicating with DataNode for streaming writes/reads
  public static int READ_TIMEOUT = 60 * 1000;
  public static int READ_TIMEOUT_EXTENSION = 5 * 1000;
  public static int WRITE_TIMEOUT = 8 * 60 * 1000;
  public static int WRITE_TIMEOUT_EXTENSION = 5 * 1000; //for write pipeline

  /**
   * Defines the NameNode role.
   */
  static public enum NamenodeRole {
    NAMENODE  ("NameNode"),
    BACKUP    ("Backup Node"),
    CHECKPOINT("Checkpoint Node");

    private String description = null;
    private NamenodeRole(String arg) {this.description = arg;}
  
    @Override
    public String toString() {
      return description;
    }
  }

  /**
   * Block replica states, which it can go through while being constructed.
   */
  static public enum ReplicaState {
    /** Replica is finalized. The state when replica is not modified. */
    FINALIZED(0),
    /** Replica is being written to. */
    RBW(1),
    /** Replica is waiting to be recovered. */
    RWR(2),
    /** Replica is under recovery. */
    RUR(3),
    /** Temporary replica: created for replication and relocation only. */
    TEMPORARY(4);

    private int value;

    private ReplicaState(int v) {
      value = v;
    }

    public int getValue() {
      return value;
    }

    public static ReplicaState getState(int v) {
      return ReplicaState.values()[v];
    }

    /** Read from in */
    public static ReplicaState read(DataInput in) throws IOException {
      return values()[in.readByte()];
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.writeByte(ordinal());
    }
  }

  /**
   * States, which a block can go through while it is under construction.
   */
  static public enum BlockUCState {
    /**
     * Block construction completed.<br>
     * The block has at least one {@link ReplicaState#FINALIZED} replica,
     * and is not going to be modified.
     */
    COMPLETE,
    /**
     * The block is under construction.<br>
     * It has been recently allocated for write or append.
     */
    UNDER_CONSTRUCTION,
    /**
     * The block is under recovery.<br>
     * When a file lease expires its last block may not be {@link #COMPLETE}
     * and needs to go through a recovery procedure, 
     * which synchronizes the existing replicas contents.
     */
    UNDER_RECOVERY,
    /**
     * The block is committed.<br>
     * The client reported that all bytes are written to data-nodes
     * with the given generation stamp and block length, but no 
     * {@link ReplicaState#FINALIZED} 
     * replicas has yet been reported by data-nodes themselves.
     */
    COMMITTED;
  }
  
  public static final String NAMENODE_LEASE_HOLDER = "HDFS_NameNode";
  public static final long NAMENODE_LEASE_RECHECK_INTERVAL = 2000;
}

