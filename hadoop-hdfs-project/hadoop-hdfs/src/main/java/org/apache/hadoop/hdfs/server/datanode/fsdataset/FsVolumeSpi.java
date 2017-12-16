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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;

/**
 * This is an interface for the underlying volume.
 */
public interface FsVolumeSpi
    extends Checkable<FsVolumeSpi.VolumeCheckContext, VolumeCheckResult> {

  /**
   * Obtain a reference object that had increased 1 reference count of the
   * volume.
   *
   * It is caller's responsibility to close {@link FsVolumeReference} to decrease
   * the reference count on the volume.
   */
  FsVolumeReference obtainReference() throws ClosedChannelException;

  /** @return the StorageUuid of the volume */
  String getStorageID();

  /** @return a list of block pools. */
  String[] getBlockPoolList();

  /** @return the available storage space in bytes. */
  long getAvailable() throws IOException;

  /** @return the base path to the volume */
  URI getBaseURI();

  DF getUsageStats(Configuration conf);

  /** @return the {@link StorageLocation} to the volume */
  StorageLocation getStorageLocation();

  /** @return the {@link StorageType} of the volume */
  StorageType getStorageType();

  /** Returns true if the volume is NOT backed by persistent storage. */
  boolean isTransientStorage();

  /**
   * Reserve disk space for a block (RBW or Re-replicating)
   * so a writer does not run out of space before the block is full.
   */
  void reserveSpaceForReplica(long bytesToReserve);

  /**
   * Release disk space previously reserved for block opened for write.
   */
  void releaseReservedSpace(long bytesToRelease);

  /**
   * Release reserved memory for an RBW block written to transient storage
   * i.e. RAM.
   * bytesToRelease will be rounded down to the OS page size since locked
   * memory reservation must always be a multiple of the page size.
   */
  void releaseLockedMemory(long bytesToRelease);

  /**
   * BlockIterator will return ExtendedBlock entries from a block pool in
   * this volume.  The entries will be returned in sorted order.<p/>
   *
   * BlockIterator objects themselves do not always have internal
   * synchronization, so they can only safely be used by a single thread at a
   * time.<p/>
   *
   * Closing the iterator does not save it.  You must call save to save it.
   */
  interface BlockIterator extends Closeable {
    /**
     * Get the next block.<p/>
     *
     * Note that this block may be removed in between the time we list it,
     * and the time the caller tries to use it, or it may represent a stale
     * entry.  Callers should handle the case where the returned block no
     * longer exists.
     *
     * @return               The next block, or null if there are no
     *                         more blocks.  Null if there was an error
     *                         determining the next block.
     *
     * @throws IOException   If there was an error getting the next block in
     *                         this volume.  In this case, EOF will be set on
     *                         the iterator.
     */
    ExtendedBlock nextBlock() throws IOException;

    /**
     * Returns true if we got to the end of the block pool.
     */
    boolean atEnd();

    /**
     * Repositions the iterator at the beginning of the block pool.
     */
    void rewind();

    /**
     * Save this block iterator to the underlying volume.
     * Any existing saved block iterator with this name will be overwritten.
     * maxStalenessMs will not be saved.
     *
     * @throws IOException   If there was an error when saving the block
     *                         iterator.
     */
    void save() throws IOException;

    /**
     * Set the maximum staleness of entries that we will return.<p/>
     *
     * A maximum staleness of 0 means we will never return stale entries; a
     * larger value will allow us to reduce resource consumption in exchange
     * for returning more potentially stale entries.  Even with staleness set
     * to 0, consumers of this API must handle race conditions where block
     * disappear before they can be processed.
     */
    void setMaxStalenessMs(long maxStalenessMs);

    /**
     * Get the wall-clock time, measured in milliseconds since the Epoch,
     * when this iterator was created.
     */
    long getIterStartMs();

    /**
     * Get the wall-clock time, measured in milliseconds since the Epoch,
     * when this iterator was last saved.  Returns iterStartMs if the
     * iterator was never saved.
     */
    long getLastSavedMs();

    /**
     * Get the id of the block pool which this iterator traverses.
     */
    String getBlockPoolId();
  }

  /**
   * Create a new block iterator.  It will start at the beginning of the
   * block set.
   *
   * @param bpid             The block pool id to iterate over.
   * @param name             The name of the block iterator to create.
   *
   * @return                 The new block iterator.
   */
  BlockIterator newBlockIterator(String bpid, String name);

  /**
   * Load a saved block iterator.
   *
   * @param bpid             The block pool id to iterate over.
   * @param name             The name of the block iterator to load.
   *
   * @return                 The saved block iterator.
   * @throws IOException     If there was an IO error loading the saved
   *                           block iterator.
   */
  BlockIterator loadBlockIterator(String bpid, String name) throws IOException;

  /**
   * Get the FSDatasetSpi which this volume is a part of.
   */
  FsDatasetSpi getDataset();

  /**
   * Tracks the files and other information related to a block on the disk
   * Missing file is indicated by setting the corresponding member
   * to null.
   *
   * Because millions of these structures may be created, we try to save
   * memory here.  So instead of storing full paths, we store path suffixes.
   * The block file, if it exists, will have a path like this:
   * <volume_base_path>/<block_path>
   * So we don't need to store the volume path, since we already know what the
   * volume is.
   *
   * The metadata file, if it exists, will have a path like this:
   * <volume_base_path>/<block_path>_<genstamp>.meta
   * So if we have a block file, there isn't any need to store the block path
   * again.
   *
   * The accessor functions take care of these manipulations.
   */
  public static class ScanInfo implements Comparable<ScanInfo> {
    private final long blockId;

    /**
     * The block file path, relative to the volume's base directory.
     * If there was no block file found, this may be null. If 'vol'
     * is null, then this is the full path of the block file.
     */
    private final String blockSuffix;

    /**
     * The suffix of the meta file path relative to the block file.
     * If blockSuffix is null, then this will be the entire path relative
     * to the volume base directory, or an absolute path if vol is also
     * null.
     */
    private final String metaSuffix;

    private final FsVolumeSpi volume;

    private final FileRegion fileRegion;
    /**
     * Get the file's length in async block scan
     */
    private final long blockLength;

    private final static Pattern CONDENSED_PATH_REGEX =
        Pattern.compile("(?<!^)(\\\\|/){2,}");

    private final static String QUOTED_FILE_SEPARATOR =
        Matcher.quoteReplacement(File.separator);

    /**
     * Get the most condensed version of the path.
     *
     * For example, the condensed version of /foo//bar is /foo/bar
     * Unlike {@link File#getCanonicalPath()}, this will never perform I/O
     * on the filesystem.
     *
     * @param path the path to condense
     * @return the condensed path
     */
    private static String getCondensedPath(String path) {
      return CONDENSED_PATH_REGEX.matcher(path).
          replaceAll(QUOTED_FILE_SEPARATOR);
    }

    /**
     * Get a path suffix.
     *
     * @param f            The file to get the suffix for.
     * @param prefix       The prefix we're stripping off.
     *
     * @return             A suffix such that prefix + suffix = path to f
     */
    private static String getSuffix(File f, String prefix) {
      String fullPath = getCondensedPath(f.getAbsolutePath());
      if (fullPath.startsWith(prefix)) {
        return fullPath.substring(prefix.length());
      }
      throw new RuntimeException(prefix + " is not a prefix of " + fullPath);
    }

    /**
     * Create a ScanInfo object for a block. This constructor will examine
     * the block data and meta-data files.
     *
     * @param blockId the block ID
     * @param blockFile the path to the block data file
     * @param metaFile the path to the block meta-data file
     * @param vol the volume that contains the block
     */
    public ScanInfo(long blockId, File blockFile, File metaFile,
        FsVolumeSpi vol) {
      this.blockId = blockId;
      String condensedVolPath =
          (vol == null || vol.getBaseURI() == null) ? null :
              getCondensedPath(new File(vol.getBaseURI()).getAbsolutePath());
      this.blockSuffix = blockFile == null ? null :
              getSuffix(blockFile, condensedVolPath);
      this.blockLength = (blockFile != null) ? blockFile.length() : 0;
      if (metaFile == null) {
        this.metaSuffix = null;
      } else if (blockFile == null) {
        this.metaSuffix = getSuffix(metaFile, condensedVolPath);
      } else {
        this.metaSuffix = getSuffix(metaFile,
            condensedVolPath + blockSuffix);
      }
      this.volume = vol;
      this.fileRegion = null;
    }

    /**
     * Create a ScanInfo object for a block. This constructor will examine
     * the block data and meta-data files.
     *
     * @param blockId the block ID
     * @param vol the volume that contains the block
     * @param fileRegion the file region (for provided blocks)
     * @param length the length of the block data
     */
    public ScanInfo(long blockId, FsVolumeSpi vol, FileRegion fileRegion,
        long length) {
      this.blockId = blockId;
      this.blockLength = length;
      this.volume = vol;
      this.fileRegion = fileRegion;
      this.blockSuffix = null;
      this.metaSuffix = null;
    }

    /**
     * Returns the block data file.
     *
     * @return the block data file
     */
    public File getBlockFile() {
      return (blockSuffix == null) ? null :
        new File(new File(volume.getBaseURI()).getAbsolutePath(), blockSuffix);
    }

    /**
     * Return the length of the data block. The length returned is the length
     * cached when this object was created.
     *
     * @return the length of the data block
     */
    public long getBlockLength() {
      return blockLength;
    }

    /**
     * Returns the block meta data file or null if there isn't one.
     *
     * @return the block meta data file
     */
    public File getMetaFile() {
      if (metaSuffix == null) {
        return null;
      } else if (blockSuffix == null) {
        return new File(new File(volume.getBaseURI()).getAbsolutePath(),
            metaSuffix);
      } else {
        return new File(new File(volume.getBaseURI()).getAbsolutePath(),
            blockSuffix + metaSuffix);
      }
    }

    /**
     * Returns the block ID.
     *
     * @return the block ID
     */
    public long getBlockId() {
      return blockId;
    }

    /**
     * Returns the volume that contains the block that this object describes.
     *
     * @return the volume
     */
    public FsVolumeSpi getVolume() {
      return volume;
    }

    @Override // Comparable
    public int compareTo(ScanInfo b) {
      if (blockId < b.blockId) {
        return -1;
      } else if (blockId == b.blockId) {
        return 0;
      } else {
        return 1;
      }
    }

    @Override // Object
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ScanInfo)) {
        return false;
      }
      return blockId == ((ScanInfo) o).blockId;
    }

    @Override // Object
    public int hashCode() {
      return (int)(blockId^(blockId>>>32));
    }

    public long getGenStamp() {
      return metaSuffix != null ? Block.getGenerationStamp(
          getMetaFile().getName()) :
            HdfsConstants.GRANDFATHER_GENERATION_STAMP;
    }

    public FileRegion getFileRegion() {
      return fileRegion;
    }
  }

  /**
   * Load last partial chunk checksum from checksum file.
   * Need to be called with FsDataset lock acquired.
   * @param blockFile
   * @param metaFile
   * @return the last partial checksum
   * @throws IOException
   */
  byte[] loadLastPartialChunkChecksum(File blockFile, File metaFile)
      throws IOException;

  /**
   * Compile a list of {@link ScanInfo} for the blocks in
   * the block pool with id {@code bpid}.
   *
   * @param bpid block pool id to scan
   * @param report the list onto which blocks reports are placed
   * @param reportCompiler
   * @throws IOException
   */
  LinkedList<ScanInfo> compileReport(String bpid,
      LinkedList<ScanInfo> report, ReportCompiler reportCompiler)
      throws InterruptedException, IOException;

  /**
   * Context for the {@link #check} call.
   */
  class VolumeCheckContext {
  }

  FileIoProvider getFileIoProvider();

  DataNodeVolumeMetrics getMetrics();
}
