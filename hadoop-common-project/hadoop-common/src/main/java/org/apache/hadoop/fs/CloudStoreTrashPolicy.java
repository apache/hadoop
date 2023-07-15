/*
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

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Time;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.bindToDurationTrackerFactory;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.TRASH_CREATE_CHECKPOINT;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.TRASH_DELETE_CHECKPOINT;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.TRASH_MOVE_TO_TRASH;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.util.functional.RemoteIterators.foreach;
import static org.apache.hadoop.util.functional.RemoteIterators.remoteIteratorFromIterable;

/**
 * A Cloud Store Trash Policy designed to be resilient to
 * race conditions and configurable to automatically clean up
 * the current user's older checkpoints whenever invoked.
 *
 *
 * The duration of trash operations are tracked in
 * the target FileSystem's statistics, if it is configured
 * to track these statistics:
 * <ul>
 *   <li>{@link org.apache.hadoop.fs.statistics.StoreStatisticNames#TRASH_CREATE_CHECKPOINT}</li>
 *   <li>{@link org.apache.hadoop.fs.statistics.StoreStatisticNames#TRASH_DELETE_CHECKPOINT}</li>
 *   <li>{@link org.apache.hadoop.fs.statistics.StoreStatisticNames#TRASH_MOVE_TO_TRASH}</li>
 * </ul>
 */
public class CloudStoreTrashPolicy extends TrashPolicyDefault {

  private static final Logger LOG =
      LoggerFactory.getLogger(CloudStoreTrashPolicy.class);

  /**
   * Configuration option to clean up old trash: {@value}.
   */
  public static final String CLEANUP_OLD_CHECKPOINTS = "fs.trash.cleanup.old.checkpoints";

  /**
   * Default value of {@link #CLEANUP_OLD_CHECKPOINTS}: {@value}.
   * This still requires the cleanup interval to be &gt; 0.
   */
  public static final boolean CLEANUP_OLD_CHECKPOINTS_DEFAULT = true;

  /**
   * Should old trash be cleaned up?
   */
  private boolean cleanupOldTrash;

  /**
   * Duration tracker if the FS provides one through its statistics.
   */
  private DurationTrackerFactory durationTrackerFactory;

  public CloudStoreTrashPolicy() {
  }

  public boolean cleanupOldTrash() {
    return cleanupOldTrash;
  }

  public DurationTrackerFactory getDurationTrackerFactory() {
    return durationTrackerFactory;
  }

  /**
   * Set the duration tracker factory; useful for testing.
   * @param durationTrackerFactory factory.
   */
  public void setDurationTrackerFactory(
      final DurationTrackerFactory durationTrackerFactory) {
    this.durationTrackerFactory = requireNonNull(durationTrackerFactory);
  }

  @Override
  public void initialize(final Configuration conf, final FileSystem fs) {
    super.initialize(conf, fs);
    cleanupOldTrash = getDeletionInterval() > 0
        && conf.getBoolean(CLEANUP_OLD_CHECKPOINTS,
        CLEANUP_OLD_CHECKPOINTS_DEFAULT);
    // get any duration tracker
    setDurationTrackerFactory(bindToDurationTrackerFactory(fs));
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    if (!isEnabled()) {
      return false;
    }
    boolean moved;

    if (!path.isAbsolute()) {
      // make path absolute
      path = new Path(fs.getWorkingDirectory(), path);
    }
    if (!fs.exists(path)) {
      // path doesn't actually exist.
      LOG.info("'{} was deleted before it could be moved to trash", path);
      moved = true;
    } else {

      try (DurationInfo info = new DurationInfo(LOG, true, "moveToTrash(%s)",
          path)) {

        // need for lambda expression.
        Path p = path;
        moved = invokeTrackingDuration(
            durationTrackerFactory.trackDuration(TRASH_MOVE_TO_TRASH), () ->
                super.moveToTrash(p));

      } catch (IOException e) {
        if (!fs.exists(path)) {
          // race condition with the trash setup; something else moved it.
          // note that checking for FNFE is not sufficient as this may occur in
          // the rename, at which point the exception may get downgraded.
          LOG.info("'{} was deleted before it could be moved to trash", path);
          LOG.debug("IOE raised on moveToTrash({})", path, e);
          // report success
          moved = true;
        } else {
          // source path still exists, so throw the exception and skip cleanup
          // don't bother trying to cleanup here as it will only complicate
          // error reporting
          throw e;
        }
      }
    }

    // add cleanup
    if (cleanupOldTrash()) {
      executeTrashCleanup();
    }
    return moved;
  }

  /**
   * Execute the cleanup.
   * @throws IOException failure
   */
  @VisibleForTesting
  public void executeTrashCleanup() throws IOException {
    FileSystem fs = getFileSystem();
    AtomicLong count = new AtomicLong();
    long now = Time.now();

    // list the roots, iterate through
    // expecting only one root for object stores.
    foreach(
        remoteIteratorFromIterable(fs.getTrashRoots(false)),
        trashRoot -> {
          try {
            count.addAndGet(deleteCheckpoint(trashRoot.getPath(), false));
            createCheckpoint(trashRoot.getPath(), new Date(now));
          } catch (IOException e) {
            LOG.warn("Trash caught:{} Skipping {}", e, trashRoot.getPath());
            LOG.debug("Trash caught", e);
          }
        });
    LOG.debug("Cleaned up {} checkpoints", count.get());
  }

  /**
   * Delete a checkpoint; update the duration tracker statistics.
   * @param trashRoot trash root.
   * @param deleteImmediately should all entries be deleted?
   * @return a count of delete checkpoints.
   * @throws IOException outcome
   */
  @Override
  protected int deleteCheckpoint(final Path trashRoot,
      final boolean deleteImmediately)
      throws IOException {
    return invokeTrackingDuration(
        durationTrackerFactory.trackDuration(TRASH_DELETE_CHECKPOINT),
        () -> super.deleteCheckpoint(trashRoot, deleteImmediately));
  }

  /**
   * Create a checkpoint; update the duration tracker statistics.
   * @param trashRoot trash root.
   * @param date date of checkpoint
   * @throws IOException outcome
   */
  @Override
  protected void createCheckpoint(final Path trashRoot, final Date date)
      throws IOException {
    trackDurationOfInvocation(durationTrackerFactory, TRASH_CREATE_CHECKPOINT, () ->
        super.createCheckpoint(trashRoot, date));
  }
}
