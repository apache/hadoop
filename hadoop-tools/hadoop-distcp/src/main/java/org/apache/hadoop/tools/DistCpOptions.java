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

package org.apache.hadoop.tools;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.util.DistCpUtils;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The Options class encapsulates all DistCp options.
 *
 * When you add a new option, please:
 *  - Add the field along with javadoc in DistCpOptions and its Builder
 *  - Add setter method in the {@link Builder} class
 *
 * This class is immutable.
 */
public final class DistCpOptions {
  private static final Logger LOG = LoggerFactory.getLogger(Builder.class);
  public static final int MAX_NUM_LISTSTATUS_THREADS = 40;

  /** File path (hdfs:// or file://) that contains the list of actual files to
   * copy.
   */
  private final Path sourceFileListing;

  /** List of source-paths (including wildcards) to be copied to target. */
  private final List<Path> sourcePaths;

  /** Destination path for the dist-copy. */
  private final Path targetPath;

  /** Whether data need to be committed automatically. */
  private final boolean atomicCommit;

  /** the work path for atomic commit. If null, the work
   * path would be parentOf(targetPath) + "/._WIP_" + nameOf(targetPath). */
  private final Path atomicWorkPath;

  /** Whether source and target folder contents be sync'ed up. */
  private final boolean syncFolder;

  /** Whether files only present in target should be deleted. */
  private boolean deleteMissing;

  /** Whether failures during copy be ignored. */
  private final boolean ignoreFailures;

  /** Whether files should always be overwritten on target. */
  private final boolean overwrite;

  /** Whether we want to append new data to target files. This is valid only
   * with update option and CRC is not skipped. */
  private final boolean append;

  /** Whether checksum comparison should be skipped while determining if source
   * and destination files are identical. */
  private final boolean skipCRC;

  /** Whether to run blocking or non-blocking. */
  private final boolean blocking;

  // When "-diff s1 s2 src tgt" is passed, apply forward snapshot diff (from s1
  // to s2) of source cluster to the target cluster to sync target cluster with
  // the source cluster. Referred to as "Fdiff" in the code.
  // It's required that s2 is newer than s1.
  private final boolean useDiff;

  // When "-rdiff s2 s1 src tgt" is passed, apply reversed snapshot diff (from
  // s2 to s1) of target cluster to the target cluster, so to make target
  // cluster go back to s1. Referred to as "Rdiff" in the code.
  // It's required that s2 is newer than s1, and src and tgt have exact same
  // content at their s1, if src is not the same as tgt.
  private final boolean useRdiff;

  /** Whether to log additional info (path, size) in the SKIP/COPY log. */
  private final boolean verboseLog;

  // For both -diff and -rdiff, given the example command line switches, two
  // steps are taken:
  //   1. Sync Step. This step does renaming/deletion ops in the snapshot diff,
  //      so to avoid copying files copied already but renamed later(HDFS-7535)
  //   2. Copy Step. This step copy the necessary files from src to tgt
  //      2.1 For -diff, it copies from snapshot s2 of src (HDFS-8828)
  //      2.2 For -rdiff, it copies from snapshot s1 of src, where the src
  //          could be the tgt itself (HDFS-9820).
  //

  private final String fromSnapshot;
  private final String toSnapshot;

  /** The path to a file containing a list of paths to filter out of copy. */
  private final String filtersFile;

  /** Path where output logs are stored. If not specified, it will use the
   * default value JobStagingDir/_logs and delete upon job completion. */
  private final Path logPath;

  /** Set the copy strategy to use. Should map to a strategy implementation
   * in distp-default.xml. */
  private final String copyStrategy;

  /** per map bandwidth in MB. */
  private final float mapBandwidth;

  /** The number of threads to use for listStatus. We allow max
   * {@link #MAX_NUM_LISTSTATUS_THREADS} threads. Setting numThreads to zero
   * signify we should use the value from conf properties. */
  private final int numListstatusThreads;

  /** The max number of maps to use for copy. */
  private final int maxMaps;

  /** File attributes that need to be preserved. */
  private final EnumSet<FileAttribute> preserveStatus;

  // Size of chunk in number of blocks when splitting large file into chunks
  // to copy in parallel. Default is 0 and file are not splitted.
  private final int blocksPerChunk;

  private final int copyBufferSize;

  /**
   * File attributes for preserve.
   *
   * Each enum entry uses the first char as its symbol.
   */
  public enum FileAttribute {
    REPLICATION,    // R
    BLOCKSIZE,      // B
    USER,           // U
    GROUP,          // G
    PERMISSION,     // P
    CHECKSUMTYPE,   // C
    ACL,            // A
    XATTR,          // X
    TIMES;          // T

    public static FileAttribute getAttribute(char symbol) {
      for (FileAttribute attribute : values()) {
        if (attribute.name().charAt(0) == Character.toUpperCase(symbol)) {
          return attribute;
        }
      }
      throw new NoSuchElementException("No attribute for " + symbol);
    }
  }

  private DistCpOptions(Builder builder) {
    this.sourceFileListing = builder.sourceFileListing;
    this.sourcePaths = builder.sourcePaths;
    this.targetPath = builder.targetPath;

    this.atomicCommit = builder.atomicCommit;
    this.atomicWorkPath = builder.atomicWorkPath;
    this.syncFolder = builder.syncFolder;
    this.deleteMissing = builder.deleteMissing;
    this.ignoreFailures = builder.ignoreFailures;
    this.overwrite = builder.overwrite;
    this.append = builder.append;
    this.skipCRC = builder.skipCRC;
    this.blocking = builder.blocking;

    this.useDiff = builder.useDiff;
    this.useRdiff = builder.useRdiff;
    this.fromSnapshot = builder.fromSnapshot;
    this.toSnapshot = builder.toSnapshot;

    this.filtersFile = builder.filtersFile;
    this.logPath = builder.logPath;
    this.copyStrategy = builder.copyStrategy;

    this.mapBandwidth = builder.mapBandwidth;
    this.numListstatusThreads = builder.numListstatusThreads;
    this.maxMaps = builder.maxMaps;

    this.preserveStatus = builder.preserveStatus;

    this.blocksPerChunk = builder.blocksPerChunk;

    this.copyBufferSize = builder.copyBufferSize;
    this.verboseLog = builder.verboseLog;
  }

  public Path getSourceFileListing() {
    return sourceFileListing;
  }

  public List<Path> getSourcePaths() {
    return sourcePaths == null ?
        null : Collections.unmodifiableList(sourcePaths);
  }

  public Path getTargetPath() {
    return targetPath;
  }

  public boolean shouldAtomicCommit() {
    return atomicCommit;
  }

  public Path getAtomicWorkPath() {
    return atomicWorkPath;
  }

  public boolean shouldSyncFolder() {
    return syncFolder;
  }

  public boolean shouldDeleteMissing() {
    return deleteMissing;
  }

  public boolean shouldIgnoreFailures() {
    return ignoreFailures;
  }

  public boolean shouldOverwrite() {
    return overwrite;
  }

  public boolean shouldAppend() {
    return append;
  }

  public boolean shouldSkipCRC() {
    return skipCRC;
  }

  public boolean shouldBlock() {
    return blocking;
  }

  public boolean shouldUseDiff() {
    return this.useDiff;
  }

  public boolean shouldUseRdiff() {
    return this.useRdiff;
  }

  public boolean shouldUseSnapshotDiff() {
    return shouldUseDiff() || shouldUseRdiff();
  }

  public String getFromSnapshot() {
    return this.fromSnapshot;
  }

  public String getToSnapshot() {
    return this.toSnapshot;
  }

  public String getFiltersFile() {
    return filtersFile;
  }

  public Path getLogPath() {
    return logPath;
  }

  public String getCopyStrategy() {
    return copyStrategy;
  }

  public int getNumListstatusThreads() {
    return numListstatusThreads;
  }

  public int getMaxMaps() {
    return maxMaps;
  }

  public float getMapBandwidth() {
    return mapBandwidth;
  }

  public Set<FileAttribute> getPreserveAttributes() {
    return (preserveStatus == null)
        ? null
        : Collections.unmodifiableSet(preserveStatus);
  }

  /**
   * Checks if the input attribute should be preserved or not.
   *
   * @param attribute - Attribute to check
   * @return True if attribute should be preserved, false otherwise
   */
  public boolean shouldPreserve(FileAttribute attribute) {
    return preserveStatus.contains(attribute);
  }

  public int getBlocksPerChunk() {
    return blocksPerChunk;
  }

  public int getCopyBufferSize() {
    return copyBufferSize;
  }

  public boolean shouldVerboseLog() {
    return verboseLog;
  }

  /**
   * Add options to configuration. These will be used in the Mapper/committer
   *
   * @param conf - Configuration object to which the options need to be added
   */
  public void appendToConf(Configuration conf) {
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.ATOMIC_COMMIT,
        String.valueOf(atomicCommit));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.IGNORE_FAILURES,
        String.valueOf(ignoreFailures));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.SYNC_FOLDERS,
        String.valueOf(syncFolder));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.DELETE_MISSING,
        String.valueOf(deleteMissing));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.OVERWRITE,
        String.valueOf(overwrite));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.APPEND,
        String.valueOf(append));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.DIFF,
        String.valueOf(useDiff));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.RDIFF,
        String.valueOf(useRdiff));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.SKIP_CRC,
        String.valueOf(skipCRC));
    if (mapBandwidth > 0) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.BANDWIDTH,
          String.valueOf(mapBandwidth));
    }
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));
    if (filtersFile != null) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.FILTERS,
          filtersFile);
    }
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.BLOCKS_PER_CHUNK,
        String.valueOf(blocksPerChunk));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.COPY_BUFFER_SIZE,
        String.valueOf(copyBufferSize));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.VERBOSE_LOG,
        String.valueOf(verboseLog));
  }

  /**
   * Utility to easily string-ify Options, for logging.
   *
   * @return String representation of the Options.
   */
  @Override
  public String toString() {
    return "DistCpOptions{" +
        "atomicCommit=" + atomicCommit +
        ", syncFolder=" + syncFolder +
        ", deleteMissing=" + deleteMissing +
        ", ignoreFailures=" + ignoreFailures +
        ", overwrite=" + overwrite +
        ", append=" + append +
        ", useDiff=" + useDiff +
        ", useRdiff=" + useRdiff +
        ", fromSnapshot=" + fromSnapshot +
        ", toSnapshot=" + toSnapshot +
        ", skipCRC=" + skipCRC +
        ", blocking=" + blocking +
        ", numListstatusThreads=" + numListstatusThreads +
        ", maxMaps=" + maxMaps +
        ", mapBandwidth=" + mapBandwidth +
        ", copyStrategy='" + copyStrategy + '\'' +
        ", preserveStatus=" + preserveStatus +
        ", atomicWorkPath=" + atomicWorkPath +
        ", logPath=" + logPath +
        ", sourceFileListing=" + sourceFileListing +
        ", sourcePaths=" + sourcePaths +
        ", targetPath=" + targetPath +
        ", filtersFile='" + filtersFile + '\'' +
        ", blocksPerChunk=" + blocksPerChunk +
        ", copyBufferSize=" + copyBufferSize +
        ", verboseLog=" + verboseLog +
        '}';
  }

  /**
   * The builder of the {@link DistCpOptions}.
   *
   * This is designed to be the only public interface to create a
   * {@link DistCpOptions} object for users. It follows a simple Builder design
   * pattern.
   */
  public static class Builder {
    private Path sourceFileListing;
    private List<Path> sourcePaths;
    private Path targetPath;

    private boolean atomicCommit = false;
    private Path atomicWorkPath;
    private boolean syncFolder = false;
    private boolean deleteMissing = false;
    private boolean ignoreFailures = false;
    private boolean overwrite = false;
    private boolean append = false;
    private boolean skipCRC = false;
    private boolean blocking = true;
    private boolean verboseLog = false;

    private boolean useDiff = false;
    private boolean useRdiff = false;
    private String fromSnapshot;
    private String toSnapshot;

    private String filtersFile;

    private Path logPath;
    private String copyStrategy = DistCpConstants.UNIFORMSIZE;

    private int numListstatusThreads = 0;  // 0 indicates that flag is not set.
    private int maxMaps = DistCpConstants.DEFAULT_MAPS;
    private float mapBandwidth = 0; // 0 indicates we should use the default

    private EnumSet<FileAttribute> preserveStatus =
        EnumSet.noneOf(FileAttribute.class);

    private int blocksPerChunk = 0;

    private int copyBufferSize =
            DistCpConstants.COPY_BUFFER_SIZE_DEFAULT;

    public Builder(List<Path> sourcePaths, Path targetPath) {
      Preconditions.checkArgument(sourcePaths != null && !sourcePaths.isEmpty(),
          "Source paths should not be null or empty!");
      Preconditions.checkArgument(targetPath != null,
          "Target path should not be null!");
      this.sourcePaths = sourcePaths;
      this.targetPath = targetPath;
    }

    public Builder(Path sourceFileListing, Path targetPath) {
      Preconditions.checkArgument(sourceFileListing != null,
          "Source file listing should not be null!");
      Preconditions.checkArgument(targetPath != null,
          "Target path should not be null!");

      this.sourceFileListing = sourceFileListing;
      this.targetPath = targetPath;
    }

    /**
     * This is the single entry point for constructing DistCpOptions objects.
     *
     * Before a new DistCpOptions object is returned, it will set the dependent
     * options, validate the option combinations. After constructing, the
     * DistCpOptions instance is immutable.
     */
    public DistCpOptions build() {
      setOptionsForSplitLargeFile();

      validate();

      return new DistCpOptions(this);
    }

    /**
     * Override options for split large files.
     */
    private void setOptionsForSplitLargeFile() {
      if (blocksPerChunk <= 0) {
        return;
      }

      LOG.info("Enabling preserving blocksize since "
          + DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch() + " is passed.");
      preserve(FileAttribute.BLOCKSIZE);

      LOG.info("Set " + DistCpOptionSwitch.APPEND.getSwitch()
          + " to false since " + DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch()
          + " is passed.");
      this.append = false;
    }

    private void validate() {
      if ((useDiff || useRdiff) && deleteMissing) {
        // -delete and -diff/-rdiff are mutually exclusive.
        throw new IllegalArgumentException("-delete and -diff/-rdiff are "
            + "mutually exclusive. The -delete option will be ignored.");
      }

      if (!atomicCommit && atomicWorkPath != null) {
        throw new IllegalArgumentException(
            "-tmp work-path can only be specified along with -atomic");
      }

      if (syncFolder && atomicCommit) {
        throw new IllegalArgumentException("Atomic commit can't be used with "
            + "sync folder or overwrite options");
      }

      if (deleteMissing && !(overwrite || syncFolder)) {
        throw new IllegalArgumentException("Delete missing is applicable "
            + "only with update or overwrite options");
      }

      if (overwrite && syncFolder) {
        throw new IllegalArgumentException("Overwrite and update options are "
            + "mutually exclusive");
      }

      if (!syncFolder && append) {
        throw new IllegalArgumentException(
            "Append is valid only with update options");
      }
      if (skipCRC && append) {
        throw new IllegalArgumentException(
            "Append is disallowed when skipping CRC");
      }
      if (!syncFolder && (useDiff || useRdiff)) {
        throw new IllegalArgumentException(
            "-diff/-rdiff is valid only with -update option");
      }

      if (useDiff || useRdiff) {
        if (StringUtils.isBlank(fromSnapshot) ||
            StringUtils.isBlank(toSnapshot)) {
          throw new IllegalArgumentException(
              "Must provide both the starting and ending " +
                  "snapshot names for -diff/-rdiff");
        }
      }
      if (useDiff && useRdiff) {
        throw new IllegalArgumentException(
            "-diff and -rdiff are mutually exclusive");
      }

      if (verboseLog && logPath == null) {
        throw new IllegalArgumentException(
            "-v is valid only with -log option");
      }
    }

    @VisibleForTesting
    Builder withSourcePaths(List<Path> newSourcePaths) {
      this.sourcePaths = newSourcePaths;
      return this;
    }

    public Builder withAtomicCommit(boolean newAtomicCommit) {
      this.atomicCommit = newAtomicCommit;
      return this;
    }

    public Builder withAtomicWorkPath(Path newAtomicWorkPath) {
      this.atomicWorkPath = newAtomicWorkPath;
      return this;
    }

    public Builder withSyncFolder(boolean newSyncFolder) {
      this.syncFolder = newSyncFolder;
      return this;
    }

    public Builder withDeleteMissing(boolean newDeleteMissing) {
      this.deleteMissing = newDeleteMissing;
      return this;
    }

    public Builder withIgnoreFailures(boolean newIgnoreFailures) {
      this.ignoreFailures = newIgnoreFailures;
      return this;
    }

    public Builder withOverwrite(boolean newOverwrite) {
      this.overwrite = newOverwrite;
      return this;
    }

    public Builder withAppend(boolean newAppend) {
      this.append = newAppend;
      return this;
    }

    public Builder withCRC(boolean newSkipCRC) {
      this.skipCRC = newSkipCRC;
      return this;
    }

    public Builder withBlocking(boolean newBlocking) {
      this.blocking = newBlocking;
      return this;
    }

    public Builder withUseDiff(String newFromSnapshot,  String newToSnapshot) {
      this.useDiff = true;
      this.fromSnapshot = newFromSnapshot;
      this.toSnapshot = newToSnapshot;
      return this;
    }

    public Builder withUseRdiff(String newFromSnapshot, String newToSnapshot) {
      this.useRdiff = true;
      this.fromSnapshot = newFromSnapshot;
      this.toSnapshot = newToSnapshot;
      return this;
    }

    public Builder withFiltersFile(String newFiletersFile) {
      this.filtersFile = newFiletersFile;
      return this;
    }

    public Builder withLogPath(Path newLogPath) {
      this.logPath = newLogPath;
      return this;
    }

    public Builder withCopyStrategy(String newCopyStrategy) {
      this.copyStrategy = newCopyStrategy;
      return this;
    }

    public Builder withMapBandwidth(float newMapBandwidth) {
      Preconditions.checkArgument(newMapBandwidth > 0,
          "Bandwidth " + newMapBandwidth + " is invalid (should be > 0)");
      this.mapBandwidth = newMapBandwidth;
      return this;
    }

    public Builder withNumListstatusThreads(int newNumListstatusThreads) {
      if (newNumListstatusThreads > MAX_NUM_LISTSTATUS_THREADS) {
        this.numListstatusThreads = MAX_NUM_LISTSTATUS_THREADS;
      } else if (newNumListstatusThreads > 0) {
        this.numListstatusThreads = newNumListstatusThreads;
      } else {
        this.numListstatusThreads = 0;
      }
      return this;
    }

    public Builder maxMaps(int newMaxMaps) {
      this.maxMaps = Math.max(newMaxMaps, 1);
      return this;
    }

    public Builder preserve(String attributes) {
      if (attributes == null || attributes.isEmpty()) {
        preserveStatus = EnumSet.allOf(FileAttribute.class);
      } else {
        for (int index = 0; index < attributes.length(); index++) {
          preserveStatus.add(FileAttribute.
              getAttribute(attributes.charAt(index)));
        }
      }
      return this;
    }

    public Builder preserve(FileAttribute attribute) {
      preserveStatus.add(attribute);
      return this;
    }

    public Builder withBlocksPerChunk(int newBlocksPerChunk) {
      this.blocksPerChunk = newBlocksPerChunk;
      return this;
    }

    public Builder withCopyBufferSize(int newCopyBufferSize) {
      this.copyBufferSize =
          newCopyBufferSize > 0 ? newCopyBufferSize
              : DistCpConstants.COPY_BUFFER_SIZE_DEFAULT;
      return this;
    }

    public Builder withVerboseLog(boolean newVerboseLog) {
      this.verboseLog = newVerboseLog;
      return this;
    }
  }

}
