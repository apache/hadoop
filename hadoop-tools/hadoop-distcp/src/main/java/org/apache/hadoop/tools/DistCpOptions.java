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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.util.DistCpUtils;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The Options class encapsulates all DistCp options.
 * These may be set from command-line (via the OptionsParser)
 * or may be set manually.
 */
public class DistCpOptions {

  private boolean atomicCommit = false;
  private boolean syncFolder = false;
  private boolean deleteMissing = false;
  private boolean ignoreFailures = false;
  private boolean overwrite = false;
  private boolean append = false;
  private boolean skipCRC = false;
  private boolean blocking = true;
  // When "-diff s1 s2 src tgt" is passed, apply forward snapshot diff (from s1
  // to s2) of source cluster to the target cluster to sync target cluster with
  // the source cluster. Referred to as "Fdiff" in the code.
  // It's required that s2 is newer than s1.
  private boolean useDiff = false;

  // When "-rdiff s2 s1 src tgt" is passed, apply reversed snapshot diff (from
  // s2 to s1) of target cluster to the target cluster, so to make target
  // cluster go back to s1. Referred to as "Rdiff" in the code.
  // It's required that s2 is newer than s1, and src and tgt have exact same
  // content at their s1, if src is not the same as tgt.
  private boolean useRdiff = false;

  // For both -diff and -rdiff, given the example command line switches, two
  // steps are taken:
  //   1. Sync Step. This step does renaming/deletion ops in the snapshot diff,
  //      so to avoid copying files copied already but renamed later(HDFS-7535)
  //   2. Copy Step. This step copy the necessary files from src to tgt
  //      2.1 For -diff, it copies from snapshot s2 of src (HDFS-8828)
  //      2.2 For -rdiff, it copies from snapshot s1 of src, where the src
  //          could be the tgt itself (HDFS-9820).
  //

  public static final int maxNumListstatusThreads = 40;
  private int numListstatusThreads = 0;  // Indicates that flag is not set.
  private int maxMaps = DistCpConstants.DEFAULT_MAPS;
  private int mapBandwidth = DistCpConstants.DEFAULT_BANDWIDTH_MB;

  private String sslConfigurationFile;

  private String copyStrategy = DistCpConstants.UNIFORMSIZE;

  private EnumSet<FileAttribute> preserveStatus = EnumSet.noneOf(FileAttribute.class);

  private boolean preserveRawXattrs;

  private Path atomicWorkPath;

  private Path logPath;

  private Path sourceFileListing;
  private List<Path> sourcePaths;

  private String fromSnapshot;
  private String toSnapshot;

  private Path targetPath;

  /**
   * The path to a file containing a list of paths to filter out of the copy.
   */
  private String filtersFile;

  // targetPathExist is a derived field, it's initialized in the
  // beginning of distcp.
  private boolean targetPathExists = true;
  
  public static enum FileAttribute{
    REPLICATION, BLOCKSIZE, USER, GROUP, PERMISSION, CHECKSUMTYPE, ACL, XATTR, TIMES;

    public static FileAttribute getAttribute(char symbol) {
      for (FileAttribute attribute : values()) {
        if (attribute.name().charAt(0) == Character.toUpperCase(symbol)) {
          return attribute;
        }
      }
      throw new NoSuchElementException("No attribute for " + symbol);
    }
  }

  /**
   * Constructor, to initialize source/target paths.
   * @param sourcePaths List of source-paths (including wildcards)
   *                     to be copied to target.
   * @param targetPath Destination path for the dist-copy.
   */
  public DistCpOptions(List<Path> sourcePaths, Path targetPath) {
    assert sourcePaths != null && !sourcePaths.isEmpty() : "Invalid source paths";
    assert targetPath != null : "Invalid Target path";

    this.sourcePaths = sourcePaths;
    this.targetPath = targetPath;
  }

  /**
   * Constructor, to initialize source/target paths.
   * @param sourceFileListing File containing list of source paths
   * @param targetPath Destination path for the dist-copy.
   */
  public DistCpOptions(Path sourceFileListing, Path targetPath) {
    assert sourceFileListing != null : "Invalid source paths";
    assert targetPath != null : "Invalid Target path";

    this.sourceFileListing = sourceFileListing;
    this.targetPath = targetPath;
  }

  /**
   * Copy constructor.
   * @param that DistCpOptions being copied from.
   */
  public DistCpOptions(DistCpOptions that) {
    if (this != that && that != null) {
      this.atomicCommit = that.atomicCommit;
      this.syncFolder = that.syncFolder;
      this.deleteMissing = that.deleteMissing;
      this.ignoreFailures = that.ignoreFailures;
      this.overwrite = that.overwrite;
      this.skipCRC = that.skipCRC;
      this.blocking = that.blocking;
      this.useDiff = that.useDiff;
      this.useRdiff = that.useRdiff;
      this.numListstatusThreads = that.numListstatusThreads;
      this.maxMaps = that.maxMaps;
      this.mapBandwidth = that.mapBandwidth;
      this.sslConfigurationFile = that.getSslConfigurationFile();
      this.copyStrategy = that.copyStrategy;
      this.preserveStatus = that.preserveStatus;
      this.preserveRawXattrs = that.preserveRawXattrs;
      this.atomicWorkPath = that.getAtomicWorkPath();
      this.logPath = that.getLogPath();
      this.sourceFileListing = that.getSourceFileListing();
      this.sourcePaths = that.getSourcePaths();
      this.targetPath = that.getTargetPath();
      this.targetPathExists = that.getTargetPathExists();
      this.filtersFile = that.getFiltersFile();
    }
  }

  /**
   * Should the data be committed atomically?
   *
   * @return true if data should be committed automically. false otherwise
   */
  public boolean shouldAtomicCommit() {
    return atomicCommit;
  }

  /**
   * Set if data need to be committed automatically
   *
   * @param atomicCommit - boolean switch
   */
  public void setAtomicCommit(boolean atomicCommit) {
    validate(DistCpOptionSwitch.ATOMIC_COMMIT, atomicCommit);
    this.atomicCommit = atomicCommit;
  }

  /**
   * Should the data be sync'ed between source and target paths?
   *
   * @return true if data should be sync'ed up. false otherwise
   */
  public boolean shouldSyncFolder() {
    return syncFolder;
  }

  /**
   * Set if source and target folder contents be sync'ed up
   *
   * @param syncFolder - boolean switch
   */
  public void setSyncFolder(boolean syncFolder) {
    validate(DistCpOptionSwitch.SYNC_FOLDERS, syncFolder);
    this.syncFolder = syncFolder;
  }

  /**
   * Should target files missing in source should be deleted?
   *
   * @return true if zoombie target files to be removed. false otherwise
   */
  public boolean shouldDeleteMissing() {
    return deleteMissing;
  }

  /**
   * Set if files only present in target should be deleted
   *
   * @param deleteMissing - boolean switch
   */
  public void setDeleteMissing(boolean deleteMissing) {
    validate(DistCpOptionSwitch.DELETE_MISSING, deleteMissing);
    this.deleteMissing = deleteMissing;
    ignoreDeleteMissingIfUseSnapshotDiff();
  }

  /**
   * -delete and -diff are mutually exclusive.
   * For backward compatibility, we ignore the -delete option here, instead of
   * throwing an IllegalArgumentException. See HDFS-10397 for more discussion.
   */
  private void ignoreDeleteMissingIfUseSnapshotDiff() {
    if (deleteMissing && (useDiff || useRdiff)) {
      OptionsParser.LOG.warn("-delete and -diff/-rdiff are mutually exclusive. " +
          "The -delete option will be ignored.");
      deleteMissing = false;
    }
  }

  /**
   * Should failures be logged and ignored during copy?
   *
   * @return true if failures are to be logged and ignored. false otherwise
   */
  public boolean shouldIgnoreFailures() {
    return ignoreFailures;
  }

  /**
   * Set if failures during copy be ignored
   *
   * @param ignoreFailures - boolean switch
   */
  public void setIgnoreFailures(boolean ignoreFailures) {
    this.ignoreFailures = ignoreFailures;
  }

  /**
   * Should DistCp be running in blocking mode
   *
   * @return true if should run in blocking, false otherwise
   */
  public boolean shouldBlock() {
    return blocking;
  }

  /**
   * Set if Disctp should run blocking or non-blocking
   *
   * @param blocking - boolean switch
   */
  public void setBlocking(boolean blocking) {
    this.blocking = blocking;
  }

  /**
   * Should files be overwritten always?
   *
   * @return true if files in target that may exist before distcp, should always
   *         be overwritten. false otherwise
   */
  public boolean shouldOverwrite() {
    return overwrite;
  }

  /**
   * Set if files should always be overwritten on target
   *
   * @param overwrite - boolean switch
   */
  public void setOverwrite(boolean overwrite) {
    validate(DistCpOptionSwitch.OVERWRITE, overwrite);
    this.overwrite = overwrite;
  }

  /**
   * @return whether we can append new data to target files
   */
  public boolean shouldAppend() {
    return append;
  }

  /**
   * Set if we want to append new data to target files. This is valid only with
   * update option and CRC is not skipped.
   */
  public void setAppend(boolean append) {
    validate(DistCpOptionSwitch.APPEND, append);
    this.append = append;
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

  public void setUseDiff(String fromSS, String toSS) {
    this.fromSnapshot = fromSS;
    this.toSnapshot = toSS;
    validate(DistCpOptionSwitch.DIFF, true);
    this.useDiff = true;
    ignoreDeleteMissingIfUseSnapshotDiff();
  }

  public void setUseRdiff(String fromSS, String toSS) {
    this.fromSnapshot = fromSS;
    this.toSnapshot = toSS;
    validate(DistCpOptionSwitch.RDIFF, true);
    this.useRdiff = true;
    ignoreDeleteMissingIfUseSnapshotDiff();
  }

  /**
   * Should CRC/checksum check be skipped while checking files are identical
   *
   * @return true if checksum check should be skipped while checking files are
   *         identical. false otherwise
   */
  public boolean shouldSkipCRC() {
    return skipCRC;
  }

  /**
   * Set if checksum comparison should be skipped while determining if
   * source and destination files are identical
   *
   * @param skipCRC - boolean switch
   */
  public void setSkipCRC(boolean skipCRC) {
    validate(DistCpOptionSwitch.SKIP_CRC, skipCRC);
    this.skipCRC = skipCRC;
  }

  /** Get the number of threads to use for listStatus
   *
   * @return Number of threads to do listStatus
   */
  public int getNumListstatusThreads() {
    return numListstatusThreads;
  }

  /** Set the number of threads to use for listStatus. We allow max 40
   *  threads. Setting numThreads to zero signify we should use the value
   *  from conf properties.
   *
   * @param numThreads - Number of threads
   */
  public void setNumListstatusThreads(int numThreads) {
    if (numThreads > maxNumListstatusThreads) {
      this.numListstatusThreads = maxNumListstatusThreads;
    } else if (numThreads > 0) {
      this.numListstatusThreads = numThreads;
    } else {
      this.numListstatusThreads = 0;
    }
  }

  /** Get the max number of maps to use for this copy
   *
   * @return Max number of maps
   */
  public int getMaxMaps() {
    return maxMaps;
  }

  /**
   * Set the max number of maps to use for copy
   *
   * @param maxMaps - Number of maps
   */
  public void setMaxMaps(int maxMaps) {
    this.maxMaps = Math.max(maxMaps, 1);
  }

  /** Get the map bandwidth in MB
   *
   * @return Bandwidth in MB
   */
  public int getMapBandwidth() {
    return mapBandwidth;
  }

  /**
   * Set per map bandwidth
   *
   * @param mapBandwidth - per map bandwidth
   */
  public void setMapBandwidth(int mapBandwidth) {
    assert mapBandwidth > 0 : "Bandwidth " + mapBandwidth + " is invalid (should be > 0)";
    this.mapBandwidth = mapBandwidth;
  }

  /**
   * Get path where the ssl configuration file is present to use for hftps://
   *
   * @return Path on local file system
   */
  public String getSslConfigurationFile() {
    return sslConfigurationFile;
  }

  /**
   * Set the SSL configuration file path to use with hftps:// (local path)
   *
   * @param sslConfigurationFile - Local ssl config file path
   */
  public void setSslConfigurationFile(String sslConfigurationFile) {
    this.sslConfigurationFile = sslConfigurationFile;
  }

  /**
   * Returns an iterator with the list of file attributes to preserve
   *
   * @return iterator of file attributes to preserve
   */
  public Iterator<FileAttribute> preserveAttributes() {
    return preserveStatus.iterator();
  }

  /**
   * Checks if the input attribute should be preserved or not
   *
   * @param attribute - Attribute to check
   * @return True if attribute should be preserved, false otherwise
   */
  public boolean shouldPreserve(FileAttribute attribute) {
    return preserveStatus.contains(attribute);
  }

  /**
   * Add file attributes that need to be preserved. This method may be
   * called multiple times to add attributes.
   *
   * @param fileAttribute - Attribute to add, one at a time
   */
  public void preserve(FileAttribute fileAttribute) {
    for (FileAttribute attribute : preserveStatus) {
      if (attribute.equals(fileAttribute)) {
        return;
      }
    }
    preserveStatus.add(fileAttribute);
  }

  /**
   * Return true if raw.* xattrs should be preserved.
   * @return true if raw.* xattrs should be preserved.
   */
  public boolean shouldPreserveRawXattrs() {
    return preserveRawXattrs;
  }

  /**
   * Indicate that raw.* xattrs should be preserved
   */
  public void preserveRawXattrs() {
    preserveRawXattrs = true;
  }

  /** Get work path for atomic commit. If null, the work
   * path would be parentOf(targetPath) + "/._WIP_" + nameOf(targetPath)
   *
   * @return Atomic work path on the target cluster. Null if not set
   */
  public Path getAtomicWorkPath() {
    return atomicWorkPath;
  }

  /**
   * Set the work path for atomic commit
   *
   * @param atomicWorkPath - Path on the target cluster
   */
  public void setAtomicWorkPath(Path atomicWorkPath) {
    this.atomicWorkPath = atomicWorkPath;
  }

  /** Get output directory for writing distcp logs. Otherwise logs
   * are temporarily written to JobStagingDir/_logs and deleted
   * upon job completion
   *
   * @return Log output path on the cluster where distcp job is run
   */
  public Path getLogPath() {
    return logPath;
  }

  /**
   * Set the log path where distcp output logs are stored
   * Uses JobStagingDir/_logs by default
   *
   * @param logPath - Path where logs will be saved
   */
  public void setLogPath(Path logPath) {
    this.logPath = logPath;
  }

  /**
   * Get the copy strategy to use. Uses appropriate input format
   *
   * @return copy strategy to use
   */
  public String getCopyStrategy() {
    return copyStrategy;
  }

  /**
   * Set the copy strategy to use. Should map to a strategy implementation
   * in distp-default.xml
   *
   * @param copyStrategy - copy Strategy to use
   */
  public void setCopyStrategy(String copyStrategy) {
    this.copyStrategy = copyStrategy;
  }

  /**
   * File path (hdfs:// or file://) that contains the list of actual
   * files to copy
   *
   * @return - Source listing file path
   */
  public Path getSourceFileListing() {
    return sourceFileListing;
  }

  /**
   * Getter for sourcePaths.
   * @return List of source-paths.
   */
  public List<Path> getSourcePaths() {
    return sourcePaths;
  }

  /**
   * Setter for sourcePaths.
   * @param sourcePaths The new list of source-paths.
   */
  public void setSourcePaths(List<Path> sourcePaths) {
    assert sourcePaths != null && sourcePaths.size() != 0;
    this.sourcePaths = sourcePaths;
  }

  /**
   * Getter for the targetPath.
   * @return The target-path.
   */
  public Path getTargetPath() {
    return targetPath;
  }

  /**
   * Getter for the targetPathExists.
   * @return The target-path.
   */
  public boolean getTargetPathExists() {
    return targetPathExists;
  }
  
  /**
   * Set targetPathExists.
   * @param targetPathExists Whether the target path of distcp exists.
   */
  public boolean setTargetPathExists(boolean targetPathExists) {
    return this.targetPathExists = targetPathExists;
  }

  /**
   * File path that contains the list of patterns
   * for paths to be filtered from the file copy.
   * @return - Filter  file path.
   */
  public final String getFiltersFile() {
    return filtersFile;
  }

  /**
   * Set filtersFile.
   * @param filtersFilename The path to a list of patterns to exclude from copy.
   */
  public final void setFiltersFile(String filtersFilename) {
    this.filtersFile = filtersFilename;
  }

  public void validate(DistCpOptionSwitch option, boolean value) {

    boolean syncFolder = (option == DistCpOptionSwitch.SYNC_FOLDERS ?
        value : this.syncFolder);
    boolean overwrite = (option == DistCpOptionSwitch.OVERWRITE ?
        value : this.overwrite);
    boolean deleteMissing = (option == DistCpOptionSwitch.DELETE_MISSING ?
        value : this.deleteMissing);
    boolean atomicCommit = (option == DistCpOptionSwitch.ATOMIC_COMMIT ?
        value : this.atomicCommit);
    boolean skipCRC = (option == DistCpOptionSwitch.SKIP_CRC ?
        value : this.skipCRC);
    boolean append = (option == DistCpOptionSwitch.APPEND ? value : this.append);
    boolean useDiff = (option == DistCpOptionSwitch.DIFF ? value : this.useDiff);
    boolean useRdiff = (option == DistCpOptionSwitch.RDIFF ? value : this.useRdiff);

    if (syncFolder && atomicCommit) {
      throw new IllegalArgumentException("Atomic commit can't be used with " +
          "sync folder or overwrite options");
    }

    if (deleteMissing && !(overwrite || syncFolder)) {
      throw new IllegalArgumentException("Delete missing is applicable " +
          "only with update or overwrite options");
    }

    if (overwrite && syncFolder) {
      throw new IllegalArgumentException("Overwrite and update options are " +
          "mutually exclusive");
    }

    if (!syncFolder && skipCRC) {
      throw new IllegalArgumentException("Skip CRC is valid only with update options");
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
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.BANDWIDTH,
        String.valueOf(mapBandwidth));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));
    if (filtersFile != null) {
      DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.FILTERS,
          filtersFile);
    }
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
        ", sslConfigurationFile='" + sslConfigurationFile + '\'' +
        ", copyStrategy='" + copyStrategy + '\'' +
        ", preserveStatus=" + preserveStatus +
        ", preserveRawXattrs=" + preserveRawXattrs +
        ", atomicWorkPath=" + atomicWorkPath +
        ", logPath=" + logPath +
        ", sourceFileListing=" + sourceFileListing +
        ", sourcePaths=" + sourcePaths +
        ", targetPath=" + targetPath +
        ", targetPathExists=" + targetPathExists +
        ", filtersFile='" + filtersFile + '\'' +
        '}';
  }

  @Override
  protected DistCpOptions clone() throws CloneNotSupportedException {
    return (DistCpOptions) super.clone();
  }
}
