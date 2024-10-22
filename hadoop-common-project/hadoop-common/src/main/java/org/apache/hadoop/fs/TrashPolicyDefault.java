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
package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CLEAN_TRASHROOT_ENABLE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CLEAN_TRASHROOT_ENABLE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TrashPolicyDefault extends TrashPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(TrashPolicyDefault.class);

  private static final Path CURRENT = new Path("Current");

  private static final FsPermission PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final DateFormat CHECKPOINT = new SimpleDateFormat("yyMMddHHmmss");
  /** Format of checkpoint directories used prior to Hadoop 0.23. */
  private static final DateFormat OLD_CHECKPOINT =
      new SimpleDateFormat("yyMMddHHmm");
  private static final int MSECS_PER_MINUTE = 60_000;

  private long emptierInterval;

  private boolean cleanNonCheckpointUnderTrashRoot;

  public TrashPolicyDefault() { }

  private TrashPolicyDefault(FileSystem fs, Configuration conf)
      throws IOException {
    initialize(conf, fs);
  }

  /**
   * @deprecated Use {@link #initialize(Configuration, FileSystem)} instead.
   */
  @Override
  @Deprecated
  public void initialize(Configuration conf, FileSystem fs, Path home) {
    this.fs = fs;
    this.deletionInterval = (long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.cleanNonCheckpointUnderTrashRoot = conf.getBoolean(
        FS_TRASH_CLEAN_TRASHROOT_ENABLE_KEY, FS_TRASH_CLEAN_TRASHROOT_ENABLE_DEFAULT);
   }

  @Override
  public void initialize(Configuration conf, FileSystem fs) {
    this.fs = fs;
    this.deletionInterval = (long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.cleanNonCheckpointUnderTrashRoot = conf.getBoolean(
        FS_TRASH_CLEAN_TRASHROOT_ENABLE_KEY, FS_TRASH_CLEAN_TRASHROOT_ENABLE_DEFAULT);
    if (deletionInterval < 0) {
      LOG.warn("Invalid value {} for deletion interval,"
          + " deletion interaval can not be negative."
          + "Changing to default value 0", deletionInterval);
      this.deletionInterval = 0;
    }
  }

  private Path makeTrashRelativePath(Path basePath, Path rmFilePath) {
    return Path.mergePaths(basePath, rmFilePath);
  }

  @Override
  public boolean isEnabled() {
    return deletionInterval > 0;
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean moveToTrash(Path path) throws IOException {
    if (!isEnabled())
      return false;

    if (!path.isAbsolute())                       // make path absolute
      path = new Path(fs.getWorkingDirectory(), path);

    // check that path exists
    fs.getFileStatus(path);
    String qpath = fs.makeQualified(path).toString();

    Path trashRoot = fs.getTrashRoot(path);
    Path trashCurrent = new Path(trashRoot, CURRENT);
    if (qpath.startsWith(trashRoot.toString())) {
      return false;                               // already in trash
    }

    if (trashRoot.getParent().toString().startsWith(qpath)) {
      throw new IOException("Cannot move \"" + path +
                            "\" to the trash, as it contains the trash");
    }

    Path trashPath = makeTrashRelativePath(trashCurrent, path);
    Path baseTrashPath = makeTrashRelativePath(trashCurrent, path.getParent());
    
    IOException cause = null;

    // try twice, in case checkpoint between the mkdirs() & rename()
    for (int i = 0; i < 2; i++) {
      try {
        if (!fs.mkdirs(baseTrashPath, PERMISSION)) {      // create current
          LOG.warn("Can't create(mkdir) trash directory: {}", baseTrashPath);
          return false;
        }
      } catch (FileAlreadyExistsException e) {
        // find the path which is not a directory, and modify baseTrashPath
        // & trashPath, then mkdirs
        Path existsFilePath = baseTrashPath;
        while (!fs.exists(existsFilePath)) {
          existsFilePath = existsFilePath.getParent();
        }
        baseTrashPath = new Path(baseTrashPath.toString().replace(
            existsFilePath.toString(), existsFilePath.toString() + Time.now())
        );
        trashPath = new Path(baseTrashPath, trashPath.getName());
        // retry, ignore current failure
        --i;
        continue;
      } catch (IOException e) {
        LOG.warn("Can't create trash directory: {}", baseTrashPath, e);
        cause = e;
        break;
      }
      try {
        // if the target path in Trash already exists, then append with 
        // a current time in millisecs.
        String orig = trashPath.toString();
        
        while(fs.exists(trashPath)) {
          trashPath = new Path(orig + Time.now());
        }
        
        // move to current trash
        fs.rename(path, trashPath,
            Rename.TO_TRASH);
        LOG.info("Moved: '{}' to trash at: {}", path, trashPath);
        return true;
      } catch (IOException e) {
        cause = e;
      }
    }
    throw new IOException("Failed to move " + path + " to trash " + trashPath,
        cause);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createCheckpoint() throws IOException {
    createCheckpoint(new Date());
  }

  @SuppressWarnings("deprecation")
  public void createCheckpoint(Date date) throws IOException {
    Collection<FileStatus> trashRoots = fs.getTrashRoots(false);
    for (FileStatus trashRoot: trashRoots) {
      LOG.info("TrashPolicyDefault#createCheckpoint for trashRoot: " +
          trashRoot.getPath());
      createCheckpoint(trashRoot.getPath(), date);
    }
  }

  @Override
  public void deleteCheckpoint() throws IOException {
    deleteCheckpoint(false);
  }

  @Override
  public void deleteCheckpointsImmediately() throws IOException {
    deleteCheckpoint(true);
  }

  private void deleteCheckpoint(boolean deleteImmediately) throws IOException {
    Collection<FileStatus> trashRoots = fs.getTrashRoots(false);
    for (FileStatus trashRoot : trashRoots) {
      LOG.info("TrashPolicyDefault#deleteCheckpoint for trashRoot: {}",
          trashRoot.getPath());
      deleteCheckpoint(trashRoot.getPath(), deleteImmediately);
    }
  }

  @Override
  public Path getCurrentTrashDir() {
    return new Path(fs.getTrashRoot(null), CURRENT);
  }

  @Override
  public Path getCurrentTrashDir(Path path) throws IOException {
    return new Path(fs.getTrashRoot(path), CURRENT);
  }

  @Override
  public Runnable getEmptier() throws IOException {
    return new Emptier(getConf(), emptierInterval);
  }

  /**
   * The emptier of this trash.
   * This thread is expected to be run on HDFS namenodes.. After being interrupted, it
   * will close the filesystem the trash policy was bonded to; see HADOOP-2337.
   * If used elsewhere, it *must* be given its own instance of the target filesystem.
   */
  protected class Emptier implements Runnable {

    private Configuration conf;
    private long emptierInterval;

    Emptier(Configuration conf, long emptierInterval) throws IOException {
      this.conf = conf;
      this.emptierInterval = emptierInterval;
      if (emptierInterval > deletionInterval || emptierInterval <= 0) {
        LOG.info("The configured checkpoint interval is {} minutes."
                + " Using an interval of {} minutes that is used for deletion instead",
            emptierInterval / MSECS_PER_MINUTE,
            deletionInterval / MSECS_PER_MINUTE);
        this.emptierInterval = deletionInterval;
      }
      LOG.info("Namenode trash configuration: Deletion interval = {} minutes."
              + " Emptier interval = {} minutes.",
          deletionInterval / MSECS_PER_MINUTE,
          emptierInterval / MSECS_PER_MINUTE);
    }

    @Override
    public void run() {
      if (emptierInterval == 0)
        return;                                   // trash disabled
      long now, end;
      while (true) {
        now = Time.now();
        end = ceiling(now, emptierInterval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          break;                                  // exit on interrupt
        }

        try {
          now = Time.now();
          if (now >= end) {
            Collection<FileStatus> trashRoots;
            trashRoots = fs.getTrashRoots(true);      // list all trash dirs

            for (FileStatus trashRoot : trashRoots) {   // dump each trash
              if (!trashRoot.isDirectory()) {
                LOG.debug("Trash root {} is not a directory: {}",
                    trashRoot.getPath(), trashRoot);
                continue;
              }
              try {
                TrashPolicyDefault trash = new TrashPolicyDefault(fs, conf);
                trash.deleteCheckpoint(trashRoot.getPath(), false);
                trash.createCheckpoint(trashRoot.getPath(), new Date(now));
              } catch (IOException e) {
                LOG.warn("Trash caught:{} Skipping {}", e, trashRoot.getPath());
              }
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e); 
        }
      }
      try {
        fs.close();
      } catch(IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      }
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

    @VisibleForTesting
    protected long getEmptierInterval() {
      return this.emptierInterval/MSECS_PER_MINUTE;
    }
  }

  protected void createCheckpoint(Path trashRoot, Date date) throws IOException {
    if (!fs.exists(new Path(trashRoot, CURRENT))) {
      return;
    }
    Path checkpointBase;
    synchronized (CHECKPOINT) {
      checkpointBase = new Path(trashRoot, CHECKPOINT.format(date));
    }
    Path checkpoint = checkpointBase;
    Path current = new Path(trashRoot, CURRENT);

    int attempt = 0;
    while (true) {
      try {
        fs.rename(current, checkpoint, Rename.NONE);
        LOG.info("Created trash checkpoint: {}", checkpoint.toUri().getPath());
        break;
      } catch (FileAlreadyExistsException e) {
        if (++attempt > 1000) {
          throw new IOException("Failed to checkpoint trash: " + checkpoint, e);
        }
        checkpoint = checkpointBase.suffix("-" + attempt);
      }
    }
  }

  /**
   * Delete trash directories under a checkpoint older than the interval,
   * or, if {@code deleteImmediately} is true, all entries.
   * It is not an error if invoked on a trash root which doesn't exist.
   * @param trashRoot trash root.
   * @param deleteImmediately should all entries be deleted
   * @return the number of entries deleted.
   * @throws IOException failure in listing or delete() calls
   */
  protected int deleteCheckpoint(Path trashRoot, boolean deleteImmediately)
      throws IOException {
    LOG.info("TrashPolicyDefault#deleteCheckpoint for trashRoot: {}", trashRoot);

    RemoteIterator<FileStatus> dirs;
    try {
      dirs = fs.listStatusIterator(trashRoot); // scan trash sub-directories
    } catch (FileNotFoundException fnfe) {
      return 0;
    }

    long now = Time.now();
    int counter = 0;
    while (dirs.hasNext()) {
      counter++;
      Path path = dirs.next().getPath();
      String dir = path.toUri().getPath();
      String name = path.getName();
      if (name.equals(CURRENT.getName())) {         // skip current
        continue;
      }

      long time;
      try {
        time = getTimeFromCheckpoint(name);
      } catch (ParseException e) {
        if (cleanNonCheckpointUnderTrashRoot) {
          fs.delete(path, true);
          LOG.warn("Unexpected item in trash: " + dir + ". Deleting.");
          continue;
        } else {
          LOG.warn("Unexpected item in trash: " + dir + ". Ignoring.");
          continue;
        }
      }

      if (((now - deletionInterval) > time) || deleteImmediately) {
        deleteCheckpoint(path);
      }
    }
    return counter;
  }

  /**
   * Delete a checkpoint
   * @param path path to delete
   * @throws IOException IO exception raised in delete call.
   */
  protected void deleteCheckpoint(final Path path) throws IOException {
    String dir = path.toUri().getPath();
    if (getFileSystem().delete(path, true)) {
      LOG.info("Deleted trash checkpoint: {}", path);
    } else {
      LOG.warn("Couldn't delete checkpoint: {}. Ignoring.", path);
    }
  }

  /**
   * parse the name of a checkpoint to extgact its timestamp.
   * Uses the Hadoop 0.23 checkpoint as well as the older version (!).
   * @param name filename
   * @return the timestamp
   * @throws ParseException the filename is not a timestamp.
   */
  protected long getTimeFromCheckpoint(String name) throws ParseException {
    long time;

    try {
      synchronized (CHECKPOINT) {
        time = CHECKPOINT.parse(name).getTime();
      }
    } catch (ParseException pe) {
      // Check for old-style checkpoint directories left over
      // after an upgrade from Hadoop 1.x
      synchronized (OLD_CHECKPOINT) {
        time = OLD_CHECKPOINT.parse(name).getTime();
      }
    }

    return time;
  }
}
