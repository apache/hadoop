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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used for all replicas which are on local storage media
 * and hence, are backed by files.
 */
abstract public class LocalReplica extends ReplicaInfo {

  /**
   * Base directory containing numerically-identified sub directories and
   * possibly blocks.
   */
  private File baseDir;

  /**
   * Whether or not this replica's parent directory includes subdirs, in which
   * case we can generate them based on the replica's block ID
   */
  private boolean hasSubdirs;

  private static final Map<String, File> internedBaseDirs = new HashMap<String, File>();

  static final Log LOG = LogFactory.getLog(LocalReplica.class);
  private final static boolean IS_NATIVE_IO_AVAIL;
  static {
    IS_NATIVE_IO_AVAIL = NativeIO.isAvailable();
    if (Path.WINDOWS && !IS_NATIVE_IO_AVAIL) {
      LOG.warn("Data node cannot fully support concurrent reading"
          + " and writing without native code extensions on Windows.");
    }
  }

  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  LocalReplica(Block block, FsVolumeSpi vol, File dir) {
    this(block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp(), vol, dir);
  }

  /**
   * Constructor
   * @param blockId block id
   * @param len replica length
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   */
  LocalReplica(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir) {
    super(vol, blockId, len, genStamp);
    setDirInternal(dir);
  }

  /**
   * Copy constructor.
   * @param from the source replica
   */
  LocalReplica(LocalReplica from) {
    this(from, from.getVolume(), from.getDir());
  }

  /**
   * Get the full path of this replica's data file.
   * @return the full path of this replica's data file
   */
  @VisibleForTesting
  public File getBlockFile() {
    return new File(getDir(), getBlockName());
  }

  /**
   * Get the full path of this replica's meta file.
   * @return the full path of this replica's meta file
   */
  @VisibleForTesting
  public File getMetaFile() {
    return new File(getDir(),
        DatanodeUtil.getMetaName(getBlockName(), getGenerationStamp()));
  }

  /**
   * Return the parent directory path where this replica is located.
   * @return the parent directory path where this replica is located
   */
  protected File getDir() {
    return hasSubdirs ? DatanodeUtil.idToBlockDir(baseDir,
        getBlockId()) : baseDir;
  }

  /**
   * Set the parent directory where this replica is located.
   * @param dir the parent directory where the replica is located
   */
  private void setDirInternal(File dir) {
    if (dir == null) {
      baseDir = null;
      return;
    }

    ReplicaDirInfo dirInfo = parseBaseDir(dir);
    this.hasSubdirs = dirInfo.hasSubidrs;

    synchronized (internedBaseDirs) {
      if (!internedBaseDirs.containsKey(dirInfo.baseDirPath)) {
        // Create a new String path of this file and make a brand new File object
        // to guarantee we drop the reference to the underlying char[] storage.
        File baseDir = new File(dirInfo.baseDirPath);
        internedBaseDirs.put(dirInfo.baseDirPath, baseDir);
      }
      this.baseDir = internedBaseDirs.get(dirInfo.baseDirPath);
    }
  }

  @VisibleForTesting
  public static class ReplicaDirInfo {
    public String baseDirPath;
    public boolean hasSubidrs;

    public ReplicaDirInfo (String baseDirPath, boolean hasSubidrs) {
      this.baseDirPath = baseDirPath;
      this.hasSubidrs = hasSubidrs;
    }
  }

  @VisibleForTesting
  public static ReplicaDirInfo parseBaseDir(File dir) {

    File currentDir = dir;
    boolean hasSubdirs = false;
    while (currentDir.getName().startsWith(DataStorage.BLOCK_SUBDIR_PREFIX)) {
      hasSubdirs = true;
      currentDir = currentDir.getParentFile();
    }

    return new ReplicaDirInfo(currentDir.getAbsolutePath(), hasSubdirs);
  }

  /**
   * Copy specified file into a temporary file. Then rename the
   * temporary file to the original name. This will cause any
   * hardlinks to the original file to be removed. The temporary
   * files are created in the same directory. The temporary files will
   * be recovered (especially on Windows) on datanode restart.
   */
  private void breakHardlinks(File file, Block b) throws IOException {
    File tmpFile = DatanodeUtil.createTmpFile(b, DatanodeUtil.getUnlinkTmpFile(file));
    try (FileInputStream in = new FileInputStream(file)) {
      try (FileOutputStream out = new FileOutputStream(tmpFile)){
        IOUtils.copyBytes(in, out, 16 * 1024);
      }
      if (file.length() != tmpFile.length()) {
        throw new IOException("Copy of file " + file + " size " + file.length()+
                              " into file " + tmpFile +
                              " resulted in a size of " + tmpFile.length());
      }
      FileUtil.replaceFile(tmpFile, file);
    } catch (IOException e) {
      boolean done = tmpFile.delete();
      if (!done) {
        DataNode.LOG.info("detachFile failed to delete temporary file " +
                          tmpFile);
      }
      throw e;
    }
  }

  /**
   * This function "breaks hardlinks" to the current replica file.
   *
   * When doing a DataNode upgrade, we create a bunch of hardlinks to each block
   * file.  This cleverly ensures that both the old and the new storage
   * directories can contain the same block file, without using additional space
   * for the data.
   *
   * However, when we want to append to the replica file, we need to "break" the
   * hardlink to ensure that the old snapshot continues to contain the old data
   * length.  If we failed to do that, we could roll back to the previous/
   * directory during a downgrade, and find that the block contents were longer
   * than they were at the time of upgrade.
   *
   * @return true only if data was copied.
   * @throws IOException
   */
  public boolean breakHardLinksIfNeeded() throws IOException {
    File file = getBlockFile();
    if (file == null || getVolume() == null) {
      throw new IOException("detachBlock:Block not found. " + this);
    }
    File meta = getMetaFile();

    int linkCount = HardLink.getLinkCount(file);
    if (linkCount > 1) {
      DataNode.LOG.info("Breaking hardlink for " + linkCount + "x-linked " +
          "block " + this);
      breakHardlinks(file, this);
    }
    if (HardLink.getLinkCount(meta) > 1) {
      breakHardlinks(meta, this);
    }
    return true;
  }

  @Override
  public URI getBlockURI() {
    return getBlockFile().toURI();
  }

  @Override
  public InputStream getDataInputStream(long seekOffset) throws IOException {

    File blockFile = getBlockFile();
    if (IS_NATIVE_IO_AVAIL) {
      return NativeIO.getShareDeleteFileInputStream(blockFile, seekOffset);
    } else {
      try {
        return FsDatasetUtil.openAndSeek(blockFile, seekOffset);
      } catch (FileNotFoundException fnfe) {
        throw new IOException("Block " + this + " is not valid. " +
            "Expected block file at " + blockFile + " does not exist.");
      }
    }
  }

  @Override
  public OutputStream getDataOutputStream(boolean append) throws IOException {
    return new FileOutputStream(getBlockFile(), append);
  }

  @Override
  public boolean blockDataExists() {
    return getBlockFile().exists();
  }

  @Override
  public boolean deleteBlockData() {
    return getBlockFile().delete();
  }

  @Override
  public long getBlockDataLength() {
    return getBlockFile().length();
  }

  @Override
  public URI getMetadataURI() {
    return getMetaFile().toURI();
  }

  @Override
  public LengthInputStream getMetadataInputStream(long offset)
      throws IOException {
    File meta = getMetaFile();
    return new LengthInputStream(
        FsDatasetUtil.openAndSeek(meta, offset), meta.length());
  }

  @Override
  public OutputStream getMetadataOutputStream(boolean append)
      throws IOException {
    return new FileOutputStream(getMetaFile(), append);
  }

  @Override
  public boolean metadataExists() {
    return getMetaFile().exists();
  }

  @Override
  public boolean deleteMetadata() {
    return getMetaFile().delete();
  }

  @Override
  public long getMetadataLength() {
    return getMetaFile().length();
  }

  @Override
  public boolean renameMeta(URI destURI) throws IOException {
    return renameFile(getMetaFile(), new File(destURI));
  }

  @Override
  public boolean renameData(URI destURI) throws IOException {
    return renameFile(getBlockFile(), new File(destURI));
  }

  private boolean renameFile(File srcfile, File destfile) throws IOException {
    try {
      NativeIO.renameTo(srcfile, destfile);
      return true;
    } catch (IOException e) {
      throw new IOException("Failed to move block file for " + this
          + " from " + srcfile + " to " + destfile.getAbsolutePath(), e);
    }
  }

  @Override
  public void updateWithReplica(StorageLocation replicaLocation) {
    // for local replicas, the replica location is assumed to be a file.
    File diskFile = null;
    try {
      diskFile = new File(replicaLocation.getUri());
    } catch (IllegalArgumentException e) {
      diskFile = null;
    }

    if (null == diskFile) {
      setDirInternal(null);
    } else {
      setDirInternal(diskFile.getParentFile());
    }
  }

  @Override
  public boolean getPinning(LocalFileSystem localFS) throws IOException {
    FileStatus fss =
        localFS.getFileStatus(new Path(getBlockFile().getAbsolutePath()));
    return fss.getPermission().getStickyBit();
  }

  @Override
  public void setPinning(LocalFileSystem localFS) throws IOException {
    File f = getBlockFile();
    Path p = new Path(f.getAbsolutePath());

    FsPermission oldPermission = localFS.getFileStatus(
        new Path(f.getAbsolutePath())).getPermission();
    //sticky bit is used for pinning purpose
    FsPermission permission = new FsPermission(oldPermission.getUserAction(),
        oldPermission.getGroupAction(), oldPermission.getOtherAction(), true);
    localFS.setPermission(p, permission);
  }

  @Override
  public void bumpReplicaGS(long newGS) throws IOException {
    long oldGS = getGenerationStamp();
    File oldmeta = getMetaFile();
    setGenerationStamp(newGS);
    File newmeta = getMetaFile();

    // rename meta file to new GS
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    try {
      // calling renameMeta on the ReplicaInfo doesn't work here
      NativeIO.renameTo(oldmeta, newmeta);
    } catch (IOException e) {
      setGenerationStamp(oldGS); // restore old GS
      throw new IOException("Block " + this + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to " + newmeta, e);
    }
  }

  @Override
  public void truncateBlock(long newLength) throws IOException {
    truncateBlock(getBlockFile(), getMetaFile(), getNumBytes(), newLength);
  }

  @Override
  public int compareWith(ScanInfo info) {
    return info.getBlockFile().compareTo(getBlockFile());
  }

  static public void truncateBlock(File blockFile, File metaFile,
      long oldlen, long newlen) throws IOException {
    LOG.info("truncateBlock: blockFile=" + blockFile
        + ", metaFile=" + metaFile
        + ", oldlen=" + oldlen
        + ", newlen=" + newlen);

    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannot truncate block to from oldlen (=" + oldlen
          + ") to newlen (=" + newlen + ")");
    }

    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1)/bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n*checksumsize;
    long lastchunkoffset = (n - 1)*bpc;
    int lastchunksize = (int)(newlen - lastchunkoffset);
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile
      blockRAF.setLength(newlen);

      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }

  @Override
  public void copyMetadata(URI destination) throws IOException {
    //for local replicas, we assume the destination URI is file
    Storage.nativeCopyFileUnbuffered(getMetaFile(),
        new File(destination), true);
  }

  @Override
  public void copyBlockdata(URI destination) throws IOException {
    //for local replicas, we assume the destination URI is file
    Storage.nativeCopyFileUnbuffered(getBlockFile(),
        new File(destination), true);
  }

}
