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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.util.StringUtils;

/************************************
 * Some handy internal HDFS constants
 *
 ************************************/

@InterfaceAudience.Private
public interface HdfsServerConstants {
  int MIN_BLOCKS_FOR_WRITE = 1;

  long LEASE_RECOVER_PERIOD = 10 * 1000; // in ms
  // We need to limit the length and depth of a path in the filesystem.
  // HADOOP-438
  // Currently we set the maximum length to 8k characters and the maximum depth
  // to 1k.
  int MAX_PATH_LENGTH = 8000;
  int MAX_PATH_DEPTH = 1000;
  // An invalid transaction ID that will never be seen in a real namesystem.
  long INVALID_TXID = -12345;
  // Number of generation stamps reserved for legacy blocks.
  long RESERVED_LEGACY_GENERATION_STAMPS = 1024L * 1024 * 1024 * 1024;
  /**
   * Current layout version for NameNode.
   * Please see {@link NameNodeLayoutVersion.Feature} on adding new layout version.
   */
  int NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  /**
   * Current layout version for DataNode.
   * Please see {@link DataNodeLayoutVersion.Feature} on adding new layout version.
   */
  int DATANODE_LAYOUT_VERSION
      = DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  /**
   * Path components that are reserved in HDFS.
   * <p>
   * .reserved is only reserved under root ("/").
   */
  String[] RESERVED_PATH_COMPONENTS = new String[] {
      HdfsConstants.DOT_SNAPSHOT_DIR,
      FSDirectory.DOT_RESERVED_STRING
  };
  byte[] DOT_SNAPSHOT_DIR_BYTES
              = DFSUtil.string2Bytes(HdfsConstants.DOT_SNAPSHOT_DIR);

  /**
   * Type of the node
   */
  enum NodeType {
    NAME_NODE,
    DATA_NODE,
    JOURNAL_NODE
  }

  /** Startup options for rolling upgrade. */
  enum RollingUpgradeStartupOption{
    ROLLBACK, STARTED;

    public String getOptionString() {
      return StartupOption.ROLLINGUPGRADE.getName() + " "
          + StringUtils.toLowerCase(name());
    }

    public boolean matches(StartupOption option) {
      return option == StartupOption.ROLLINGUPGRADE
          && option.getRollingUpgradeStartupOption() == this;
    }

    private static final RollingUpgradeStartupOption[] VALUES = values();

    static RollingUpgradeStartupOption fromString(String s) {
      if ("downgrade".equalsIgnoreCase(s)) {
        throw new IllegalArgumentException(
            "The \"downgrade\" option is no longer supported"
                + " since it may incorrectly finalize an ongoing rolling upgrade."
                + " For downgrade instruction, please see the documentation"
                + " (http://hadoop.apache.org/docs/current/hadoop-project-dist/"
                + "hadoop-hdfs/HdfsRollingUpgrade.html#Downgrade).");
      }
      for(RollingUpgradeStartupOption opt : VALUES) {
        if (opt.name().equalsIgnoreCase(s)) {
          return opt;
        }
      }
      throw new IllegalArgumentException("Failed to convert \"" + s
          + "\" to " + RollingUpgradeStartupOption.class.getSimpleName());
    }

    public static String getAllOptionString() {
      final StringBuilder b = new StringBuilder("<");
      for(RollingUpgradeStartupOption opt : VALUES) {
        b.append(StringUtils.toLowerCase(opt.name())).append("|");
      }
      b.setCharAt(b.length() - 1, '>');
      return b.toString();
    }
  }

  /** Startup options */
  enum StartupOption{
    FORMAT  ("-format"),
    CLUSTERID ("-clusterid"),
    GENCLUSTERID ("-genclusterid"),
    REGULAR ("-regular"),
    BACKUP  ("-backup"),
    CHECKPOINT("-checkpoint"),
    UPGRADE ("-upgrade"),
    ROLLBACK("-rollback"),
    ROLLINGUPGRADE("-rollingUpgrade"),
    IMPORT  ("-importCheckpoint"),
    BOOTSTRAPSTANDBY("-bootstrapStandby"),
    INITIALIZESHAREDEDITS("-initializeSharedEdits"),
    RECOVER  ("-recover"),
    FORCE("-force"),
    NONINTERACTIVE("-nonInteractive"),
    SKIPSHAREDEDITSCHECK("-skipSharedEditsCheck"),
    RENAMERESERVED("-renameReserved"),
    METADATAVERSION("-metadataVersion"),
    UPGRADEONLY("-upgradeOnly"),
    // The -hotswap constant should not be used as a startup option, it is
    // only used for StorageDirectory.analyzeStorage() in hot swap drive scenario.
    // TODO refactor StorageDirectory.analyzeStorage() so that we can do away with
    // this in StartupOption.
    HOTSWAP("-hotswap"),
    // Startup the namenode in observer mode.
    OBSERVER("-observer");

    private static final Pattern ENUM_WITH_ROLLING_UPGRADE_OPTION = Pattern.compile(
        "(\\w+)\\((\\w+)\\)");

    private final String name;
    
    // Used only with format and upgrade options
    private String clusterId = null;
    
    // Used only by rolling upgrade
    private RollingUpgradeStartupOption rollingUpgradeStartupOption;

    // Used only with format option
    private boolean isForceFormat = false;
    private boolean isInteractiveFormat = true;
    
    // Used only with recovery option
    private int force = 0;

    StartupOption(String arg) {this.name = arg;}
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
    
    public void setRollingUpgradeStartupOption(String opt) {
      Preconditions.checkState(this == ROLLINGUPGRADE);
      rollingUpgradeStartupOption = RollingUpgradeStartupOption.fromString(opt);
    }
    
    public RollingUpgradeStartupOption getRollingUpgradeStartupOption() {
      Preconditions.checkState(this == ROLLINGUPGRADE);
      return rollingUpgradeStartupOption;
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
    
    @Override
    public String toString() {
      if (this == ROLLINGUPGRADE) {
        return new StringBuilder(super.toString())
            .append("(").append(getRollingUpgradeStartupOption()).append(")")
            .toString();
      }
      return super.toString();
    }

    static public StartupOption getEnum(String value) {
      Matcher matcher = ENUM_WITH_ROLLING_UPGRADE_OPTION.matcher(value);
      if (matcher.matches()) {
        StartupOption option = StartupOption.valueOf(matcher.group(1));
        option.setRollingUpgradeStartupOption(matcher.group(2));
        return option;
      } else {
        return StartupOption.valueOf(value);
      }
    }
  }

  /**
   * Defines the NameNode role.
   */
  enum NamenodeRole {
    NAMENODE  ("NameNode"),
    BACKUP    ("Backup Node"),
    CHECKPOINT("Checkpoint Node");

    private String description = null;
    NamenodeRole(String arg) {this.description = arg;}
  
    @Override
    public String toString() {
      return description;
    }
  }

  /**
   * Block replica states, which it can go through while being constructed.
   */
  enum ReplicaState {
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

    private static final ReplicaState[] cachedValues = ReplicaState.values();

    private final int value;

    ReplicaState(int v) {
      value = v;
    }

    public int getValue() {
      return value;
    }

    public static ReplicaState getState(int v) {
      return cachedValues[v];
    }

    /** Read from in */
    public static ReplicaState read(DataInput in) throws IOException {
      return cachedValues[in.readByte()];
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.writeByte(ordinal());
    }
  }

  /**
   * States, which a block can go through while it is under construction.
   */
  enum BlockUCState {
    /**
     * Block construction completed.<br>
     * The block has at least the configured minimal replication number
     * of {@link ReplicaState#FINALIZED} replica(s), and is not going to be
     * modified.
     * NOTE, in some special cases, a block may be forced to COMPLETE state,
     * even if it doesn't have required minimal replications.
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
    COMMITTED
  }
  
  String NAMENODE_LEASE_HOLDER = "HDFS_NameNode";

  String CRYPTO_XATTR_ENCRYPTION_ZONE =
      "raw.hdfs.crypto.encryption.zone";
  String CRYPTO_XATTR_FILE_ENCRYPTION_INFO =
      "raw.hdfs.crypto.file.encryption.info";
  String SECURITY_XATTR_UNREADABLE_BY_SUPERUSER =
      "security.hdfs.unreadable.by.superuser";
  String XATTR_ERASURECODING_POLICY =
      "system.hdfs.erasurecoding.policy";

  String XATTR_SATISFY_STORAGE_POLICY = "user.hdfs.sps";

  Path MOVER_ID_PATH = new Path("/system/mover.id");

  long BLOCK_GROUP_INDEX_MASK = 15;
  byte MAX_BLOCKS_IN_GROUP = 16;
}
