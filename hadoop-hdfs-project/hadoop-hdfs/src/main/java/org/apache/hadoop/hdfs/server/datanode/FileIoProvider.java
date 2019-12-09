/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.net.SocketOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.apache.hadoop.hdfs.server.datanode.FileIoProvider.OPERATION.*;

/**
 * This class abstracts out various file IO operations performed by the
 * DataNode and invokes profiling (for collecting stats) and fault injection
 * (for testing) event hooks before and after each file IO.
 *
 * Behavior can be injected into these events by enabling the
 * profiling and/or fault injection event hooks through
 * {@link DFSConfigKeys#DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY}
 * and {@link DFSConfigKeys#DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY}.
 * These event hooks are disabled by default.
 *
 * Most functions accept an optional {@link FsVolumeSpi} parameter for
 * instrumentation/logging.
 *
 * Some methods may look redundant, especially the multiple variations of
 * move/rename/list. They exist to retain behavior compatibility for existing
 * code.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileIoProvider {
  public static final Logger LOG = LoggerFactory.getLogger(
      FileIoProvider.class);

  private final ProfilingFileIoEvents profilingEventHook;
  private final FaultInjectorFileIoEvents faultInjectorEventHook;
  private final DataNode datanode;

  private static final int LEN_INT = 4;

  /**
   * @param conf  Configuration object. May be null. When null,
   *              the event handlers are no-ops.
   * @param datanode datanode that owns this FileIoProvider. Used for
   *               IO error based volume checker callback
   */
  public FileIoProvider(@Nullable Configuration conf,
                        final DataNode datanode) {
    profilingEventHook = new ProfilingFileIoEvents(conf);
    faultInjectorEventHook = new FaultInjectorFileIoEvents(conf);
    this.datanode = datanode;
  }

  /**
   * Lists the types of file system operations. Passed to the
   * IO hooks so implementations can choose behavior based on
   * specific operations.
   */
  public enum OPERATION {
    OPEN,
    EXISTS,
    LIST,
    DELETE,
    MOVE,
    MKDIRS,
    TRANSFER,
    SYNC,
    FADVISE,
    READ,
    WRITE,
    FLUSH,
    NATIVE_COPY
  }

  /**
   * See {@link Flushable#flush()}.
   *
   * @param  volume target volume. null if unavailable.
   * @throws IOException
   */
  public void flush(
      @Nullable FsVolumeSpi volume, Flushable f) throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, FLUSH, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, FLUSH, 0);
      f.flush();
      profilingEventHook.afterFileIo(volume, FLUSH, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Sync the given {@link FileOutputStream}.
   *
   * @param  volume target volume. null if unavailable.
   * @throws IOException
   */
  public void sync(
      @Nullable FsVolumeSpi volume, FileOutputStream fos) throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, SYNC, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, SYNC, 0);
      IOUtils.fsync(fos.getChannel(), false);
      profilingEventHook.afterFileIo(volume, SYNC, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Sync the given directory changes to durable device.
   * @throws IOException
   */
  public void dirSync(@Nullable FsVolumeSpi volume, File dir)
      throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, SYNC, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, SYNC, 0);
      IOUtils.fsync(dir);
      profilingEventHook.afterFileIo(volume, SYNC, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Call sync_file_range on the given file descriptor.
   *
   * @param  volume target volume. null if unavailable.
   * @throws IOException
   */
  public void syncFileRange(
      @Nullable FsVolumeSpi volume, FileDescriptor outFd,
      long offset, long numBytes, int flags) throws NativeIOException {
    final long begin = profilingEventHook.beforeFileIo(volume, SYNC, 0);
    try {
      faultInjectorEventHook.beforeFileIo(volume, SYNC, 0);
      NativeIO.POSIX.syncFileRangeIfPossible(outFd, offset, numBytes, flags);
      profilingEventHook.afterFileIo(volume, SYNC, begin, 0);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Call posix_fadvise on the given file descriptor.
   *
   * @param  volume target volume. null if unavailable.
   * @throws IOException
   */
  public void posixFadvise(
      @Nullable FsVolumeSpi volume, String identifier, FileDescriptor outFd,
      long offset, long length, int flags) throws NativeIOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, FADVISE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, FADVISE);
      NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
          identifier, outFd, offset, length, flags);
      profilingEventHook.afterMetadataOp(volume, FADVISE, begin);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Delete a file.
   * @param volume  target volume. null if unavailable.
   * @param f  File to delete.
   * @return  true if the file was successfully deleted.
   */
  public boolean delete(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      boolean deleted = f.delete();
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
      return deleted;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Delete a file, first checking to see if it exists.
   * @param volume  target volume. null if unavailable.
   * @param f  File to delete
   * @return  true if the file was successfully deleted or if it never
   *          existed.
   */
  public boolean deleteWithExistsCheck(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      boolean deleted = !f.exists() || f.delete();
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
      if (!deleted) {
        LOG.warn("Failed to delete file {}", f);
      }
      return deleted;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Transfer data from a FileChannel to a SocketOutputStream.
   *
   * @param volume  target volume. null if unavailable.
   * @param sockOut  SocketOutputStream to write the data.
   * @param fileCh  FileChannel from which to read data.
   * @param position  position within the channel where the transfer begins.
   * @param count  number of bytes to transfer.
   * @param waitTime  returns the nanoseconds spent waiting for the socket
   *                  to become writable.
   * @param transferTime  returns the nanoseconds spent transferring data.
   * @throws IOException
   */
  public void transferToSocketFully(
      @Nullable FsVolumeSpi volume, SocketOutputStream sockOut,
      FileChannel fileCh, long position, int count,
      LongWritable waitTime, LongWritable transferTime) throws IOException {
    final long begin = profilingEventHook.beforeFileIo(volume, TRANSFER, count);
    try {
      faultInjectorEventHook.beforeFileIo(volume, TRANSFER, count);
      sockOut.transferToFully(fileCh, position, count,
          waitTime, transferTime);
      profilingEventHook.afterFileIo(volume, TRANSFER, begin, count);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a file.
   * @param volume  target volume. null if unavailable.
   * @param f  File to be created.
   * @return  true if the file does not exist and was successfully created.
   *          false if the file already exists.
   * @throws IOException
   */
  public boolean createFile(
      @Nullable FsVolumeSpi volume, File f) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      boolean created = f.createNewFile();
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return created;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileInputStream using
   * {@link FileInputStream#FileInputStream(File)}.
   *
   * Wraps the created input stream to intercept read calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @return  FileInputStream to the given file.
   * @throws  FileNotFoundException
   */
  public FileInputStream getFileInputStream(
      @Nullable FsVolumeSpi volume, File f) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fis = new WrappedFileInputStream(volume, f);
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileOutputStream using
   * {@link FileOutputStream#FileOutputStream(File, boolean)}.
   *
   * Wraps the created output stream to intercept write calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param append  if true, then bytes will be written to the end of the
   *                file rather than the beginning.
   * @return  FileOutputStream to the given file object.
   * @throws FileNotFoundException
   */
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, File f,
      boolean append) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileOutputStream fos = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fos = new WrappedFileOutputStream(volume, f, append);
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fos;
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fos);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileOutputStream using
   * {@link FileOutputStream#FileOutputStream(File, boolean)}.
   *
   * Wraps the created output stream to intercept write calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @return  FileOutputStream to the given file object.
   * @throws  FileNotFoundException
   */
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, File f) throws FileNotFoundException {
    return getFileOutputStream(volume, f, false);
  }

  /**
   * Create a FileOutputStream using
   * {@link FileOutputStream#FileOutputStream(FileDescriptor)}.
   *
   * Wraps the created output stream to intercept write calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param fd  File descriptor object.
   * @return  FileOutputStream to the given file object.
   * @throws  FileNotFoundException
   */
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, FileDescriptor fd) {
    return new WrappedFileOutputStream(volume, fd);
  }

  /**
   * Create a FileInputStream using
   * {@link NativeIO#getShareDeleteFileDescriptor}.
   * Wraps the created input stream to intercept input calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param offset  the offset position, measured in bytes from the
   *                beginning of the file, at which to set the file
   *                pointer.
   * @return FileOutputStream to the given file object.
   * @throws FileNotFoundException
   */
  public FileInputStream getShareDeleteFileInputStream(
      @Nullable FsVolumeSpi volume, File f,
      long offset) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fis = new WrappedFileInputStream(volume,
          NativeIO.getShareDeleteFileDescriptor(f, offset));
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileInputStream using
   * {@link FileInputStream#FileInputStream(File)} and position
   * it at the given offset.
   *
   * Wraps the created input stream to intercept read calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param offset  the offset position, measured in bytes from the
   *                beginning of the file, at which to set the file
   *                pointer.
   * @throws FileNotFoundException
   */
  public FileInputStream openAndSeek(
      @Nullable FsVolumeSpi volume, File f, long offset) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fis = new WrappedFileInputStream(volume,
          FsDatasetUtil.openAndSeek(f, offset));
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a RandomAccessFile using
   * {@link RandomAccessFile#RandomAccessFile(File, String)}.
   *
   * Wraps the created input stream to intercept IO calls
   * before delegating to the wrapped RandomAccessFile.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param mode  See {@link RandomAccessFile} for a description
   *              of the mode string.
   * @return RandomAccessFile representing the given file.
   * @throws FileNotFoundException
   */
  public RandomAccessFile getRandomAccessFile(
      @Nullable FsVolumeSpi volume, File f,
      String mode) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    RandomAccessFile raf = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      raf = new WrappedRandomAccessFile(volume, f, mode);
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return raf;
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(raf);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Delete the given directory using {@link FileUtil#fullyDelete(File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param dir  directory to be deleted.
   * @return true on success false on failure.
   */
  public boolean fullyDelete(@Nullable FsVolumeSpi volume, File dir) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      boolean deleted = FileUtil.fullyDelete(dir);
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
      return deleted;
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Move the src file to the target using
   * {@link FileUtil#replaceFile(File, File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param src  source path.
   * @param target  target path.
   * @throws IOException
   */
  public void replaceFile(
      @Nullable FsVolumeSpi volume, File src, File target) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, MOVE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, MOVE);
      FileUtil.replaceFile(src, target);
      profilingEventHook.afterMetadataOp(volume, MOVE, begin);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Move the src file to the target using
   * {@link Storage#rename(File, File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param src  source path.
   * @param target  target path.
   * @throws IOException
   */
  public void rename(
      @Nullable FsVolumeSpi volume, File src, File target)
      throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, MOVE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, MOVE);
      Storage.rename(src, target);
      profilingEventHook.afterMetadataOp(volume, MOVE, begin);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Move the src file to the target using
   * {@link FileUtils#moveFile(File, File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param src  source path.
   * @param target  target path.
   * @throws IOException
   */
  public void moveFile(
      @Nullable FsVolumeSpi volume, File src, File target)
      throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, MOVE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, MOVE);
      FileUtils.moveFile(src, target);
      profilingEventHook.afterMetadataOp(volume, MOVE, begin);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Move the src file to the target using
   * {@link Files#move(Path, Path, CopyOption...)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param src  source path.
   * @param target  target path.
   * @param options  See {@link Files#move} for a description
   *                of the options.
   * @throws IOException
   */
  public void move(
      @Nullable FsVolumeSpi volume, Path src, Path target,
      CopyOption... options) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, MOVE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, MOVE);
      Files.move(src, target, options);
      profilingEventHook.afterMetadataOp(volume, MOVE, begin);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * See {@link Storage#nativeCopyFileUnbuffered(File, File, boolean)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param src  an existing file to copy, must not be {@code null}
   * @param target  the new file, must not be {@code null}
   * @param preserveFileDate  true if the file date of the copy
   *                         should be the same as the original
   * @throws IOException
   */
  public void nativeCopyFileUnbuffered(
      @Nullable FsVolumeSpi volume, File src, File target,
      boolean preserveFileDate) throws IOException {
    final long length = src.length();
    final long begin = profilingEventHook.beforeFileIo(volume, NATIVE_COPY,
        length);
    try {
      faultInjectorEventHook.beforeFileIo(volume, NATIVE_COPY, length);
      Storage.nativeCopyFileUnbuffered(src, target, preserveFileDate);
      profilingEventHook.afterFileIo(volume, NATIVE_COPY, begin, length);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * See {@link File#mkdirs()}.
   *
   * @param volume target volume. null if unavailable.
   * @param dir  directory to be created.
   * @return  true only if the directory was created. false if
   *          the directory already exists.
   * @throws IOException if a directory with the given name does
   *                     not exist and could not be created.
   */
  public boolean mkdirs(
      @Nullable FsVolumeSpi volume, File dir) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, MKDIRS);
    boolean created = false;
    boolean isDirectory;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, MKDIRS);
      created = dir.mkdirs();
      isDirectory = !created && dir.isDirectory();
      profilingEventHook.afterMetadataOp(volume, MKDIRS, begin);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }

    if (!created && !isDirectory) {
      throw new IOException("Mkdirs failed to create " + dir);
    }
    return created;
  }

  /**
   * Create the target directory using {@link File#mkdirs()} only if
   * it doesn't exist already.
   *
   * @param volume  target volume. null if unavailable.
   * @param dir  directory to be created.
   * @throws IOException  if the directory could not created
   */
  public void mkdirsWithExistsCheck(
      @Nullable FsVolumeSpi volume, File dir) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, MKDIRS);
    boolean succeeded = false;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, MKDIRS);
      succeeded = dir.isDirectory() || dir.mkdirs();
      profilingEventHook.afterMetadataOp(volume, MKDIRS, begin);
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }

    if (!succeeded) {
      throw new IOException("Mkdirs failed to create " + dir);
    }
  }

  /**
   * Get a listing of the given directory using
   * {@link FileUtil#listFiles(File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param dir  Directory to be listed.
   * @return  array of file objects representing the directory entries.
   * @throws IOException
   */
  public File[] listFiles(
      @Nullable FsVolumeSpi volume, File dir) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, LIST);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, LIST);
      File[] children = FileUtil.listFiles(dir);
      profilingEventHook.afterMetadataOp(volume, LIST, begin);
      return children;
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Get a listing of the given directory using
   * {@link FileUtil#listFiles(File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param   dir directory to be listed.
   * @return  array of strings representing the directory entries.
   * @throws IOException
   */
  public String[] list(
      @Nullable FsVolumeSpi volume, File dir) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, LIST);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, LIST);
      String[] children = FileUtil.list(dir);
      profilingEventHook.afterMetadataOp(volume, LIST, begin);
      return children;
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Get a listing of the given directory using
   * {@link IOUtils#listDirectory(File, FilenameFilter)}.
   *
   * @param volume target volume. null if unavailable.
   * @param dir Directory to list.
   * @param filter {@link FilenameFilter} to filter the directory entries.
   * @throws IOException
   */
  public List<String> listDirectory(
      @Nullable FsVolumeSpi volume, File dir,
      FilenameFilter filter) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, LIST);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, LIST);
      List<String> children = IOUtils.listDirectory(dir, filter);
      profilingEventHook.afterMetadataOp(volume, LIST, begin);
      return children;
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Retrieves the number of links to the specified file.
   *
   * @param volume target volume. null if unavailable.
   * @param f file whose link count is being queried.
   * @return number of hard-links to the given file, including the
   *         given path itself.
   * @throws IOException
   */
  public int getHardLinkCount(
      @Nullable FsVolumeSpi volume, File f) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, LIST);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, LIST);
      int count = HardLink.getLinkCount(f);
      profilingEventHook.afterMetadataOp(volume, LIST, begin);
      return count;
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Check for file existence using {@link File#exists()}.
   *
   * @param volume target volume. null if unavailable.
   * @param f file object.
   * @return true if the file exists.
   */
  public boolean exists(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, EXISTS);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, EXISTS);
      boolean exists = f.exists();
      profilingEventHook.afterMetadataOp(volume, EXISTS, begin);
      return exists;
    } catch(Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * A thin wrapper over {@link FileInputStream} that allows
   * instrumenting disk IO.
   */
  private final class WrappedFileInputStream extends FileInputStream {
    private @Nullable final FsVolumeSpi volume;

    /**
     * {@inheritDoc}.
     */
    private WrappedFileInputStream(@Nullable FsVolumeSpi volume, File f)
        throws FileNotFoundException {
      super(f);
      this.volume = volume;
    }

    /**
     * {@inheritDoc}.
     */
    private WrappedFileInputStream(
        @Nullable FsVolumeSpi volume, FileDescriptor fd) {
      super(fd);
      this.volume = volume;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int read() throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, LEN_INT);
      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, LEN_INT);
        int b = super.read();
        profilingEventHook.afterFileIo(volume, READ, begin, LEN_INT);
        return b;
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int read(@Nonnull byte[] b) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, b
          .length);
      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, b.length);
        int numBytesRead = super.read(b);
        profilingEventHook.afterFileIo(volume, READ, begin, numBytesRead);
        return numBytesRead;
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, len);
      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, len);
        int numBytesRead = super.read(b, off, len);
        profilingEventHook.afterFileIo(volume, READ, begin, numBytesRead);
        return numBytesRead;
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }
  }

  /**
   * A thin wrapper over {@link FileOutputStream} that allows
   * instrumenting disk IO.
   */
  private final class WrappedFileOutputStream extends FileOutputStream {
    private @Nullable final FsVolumeSpi volume;

    /**
     * {@inheritDoc}.
     */
    private WrappedFileOutputStream(
        @Nullable FsVolumeSpi volume, File f,
        boolean append) throws FileNotFoundException {
      super(f, append);
      this.volume = volume;
    }

    /**
     * {@inheritDoc}.
     */
    private WrappedFileOutputStream(
        @Nullable FsVolumeSpi volume, FileDescriptor fd) {
      super(fd);
      this.volume = volume;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void write(int b) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE,
          LEN_INT);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, LEN_INT);
        super.write(b);
        profilingEventHook.afterFileIo(volume, WRITE, begin, LEN_INT);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void write(@Nonnull byte[] b) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE, b
          .length);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, b.length);
        super.write(b);
        profilingEventHook.afterFileIo(volume, WRITE, begin, b.length);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE, len);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, len);
        super.write(b, off, len);
        profilingEventHook.afterFileIo(volume, WRITE, begin, len);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }
  }

  /**
   * A thin wrapper over {@link FileInputStream} that allows
   * instrumenting IO.
   */
  private final class WrappedRandomAccessFile extends RandomAccessFile {
    private @Nullable final FsVolumeSpi volume;

    public WrappedRandomAccessFile(
        @Nullable FsVolumeSpi volume, File f, String mode)
        throws FileNotFoundException {
      super(f, mode);
      this.volume = volume;
    }

    @Override
    public int read() throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, LEN_INT);
      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, LEN_INT);
        int b = super.read();
        profilingEventHook.afterFileIo(volume, READ, begin, LEN_INT);
        return b;
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, len);
      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, len);
        int numBytesRead = super.read(b, off, len);
        profilingEventHook.afterFileIo(volume, READ, begin, numBytesRead);
        return numBytesRead;
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    @Override
    public int read(byte[] b) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, b
          .length);
      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, b.length);
        int numBytesRead = super.read(b);
        profilingEventHook.afterFileIo(volume, READ, begin, numBytesRead);
        return numBytesRead;
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    @Override
    public void write(int b) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE,
          LEN_INT);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, LEN_INT);
        super.write(b);
        profilingEventHook.afterFileIo(volume, WRITE, begin, LEN_INT);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    @Override
    public void write(@Nonnull byte[] b) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE, b
          .length);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, b.length);
        super.write(b);
        profilingEventHook.afterFileIo(volume, WRITE, begin, b.length);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE, len);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, len);
        super.write(b, off, len);
        profilingEventHook.afterFileIo(volume, WRITE, begin, len);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }
  }

  private void onFailure(@Nullable FsVolumeSpi volume, long begin) {
    if (datanode != null && volume != null) {
      datanode.checkDiskErrorAsync(volume);
    }
    profilingEventHook.onFailure(volume, begin);
  }
}
