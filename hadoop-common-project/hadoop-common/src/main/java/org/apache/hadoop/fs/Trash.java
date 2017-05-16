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

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/** 
 * Provides a trash facility which supports pluggable Trash policies. 
 *
 * See the implementation of the configured TrashPolicy for more
 * details.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Trash extends Configured {
  private static final org.apache.commons.logging.Log LOG =
      LogFactory.getLog(Trash.class);

  private TrashPolicy trashPolicy; // configured trash policy instance

  /** 
   * Construct a trash can accessor.
   * @param conf a Configuration
   */
  public Trash(Configuration conf) throws IOException {
    this(FileSystem.get(conf), conf);
  }

  /**
   * Construct a trash can accessor for the FileSystem provided.
   * @param fs the FileSystem
   * @param conf a Configuration
   */
  public Trash(FileSystem fs, Configuration conf) throws IOException {
    super(conf);
    trashPolicy = TrashPolicy.getInstance(conf, fs, fs.getHomeDirectory());
  }

  /**
   * In case of the symlinks or mount points, one has to move the appropriate
   * trashbin in the actual volume of the path p being deleted.
   *
   * Hence we get the file system of the fully-qualified resolved-path and
   * then move the path p to the trashbin in that volume,
   * @param fs - the filesystem of path p
   * @param p - the  path being deleted - to be moved to trasg
   * @param conf - configuration
   * @return false if the item is already in the trash or trash is disabled
   * @throws IOException on error
   */
  public static boolean moveToAppropriateTrash(FileSystem fs, Path p,
      Configuration conf) throws IOException {
    Path fullyResolvedPath = fs.resolvePath(p);
    FileSystem fullyResolvedFs =
        FileSystem.get(fullyResolvedPath.toUri(), conf);
    // If the trash interval is configured server side then clobber this
    // configuration so that we always respect the server configuration.
    try {
      long trashInterval = fullyResolvedFs.getServerDefaults(
          fullyResolvedPath).getTrashInterval();
      if (0 != trashInterval) {
        Configuration confCopy = new Configuration(conf);
        confCopy.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
            trashInterval);
        conf = confCopy;
      }
    } catch (Exception e) {
      // If we can not determine that trash is enabled server side then
      // bail rather than potentially deleting a file when trash is enabled.
      LOG.warn("Failed to get server trash configuration", e);
      throw new IOException("Failed to get server trash configuration", e);
    }
    Trash trash = new Trash(fullyResolvedFs, conf);
    boolean success = trash.moveToTrash(fullyResolvedPath);
    if (success) {
      System.out.println("Moved: '" + p + "' to trash at: " +
          trash.getCurrentTrashDir() );
    }
    return success;
  }
  
  /**
   * Returns whether the trash is enabled for this filesystem
   */
  public boolean isEnabled() {
    return trashPolicy.isEnabled();
  }

  /** Move a file or directory to the current trash directory.
   * @return false if the item is already in the trash or trash is disabled
   */ 
  public boolean moveToTrash(Path path) throws IOException {
    return trashPolicy.moveToTrash(path);
  }

  /** Create a trash checkpoint. */
  public void checkpoint() throws IOException {
    trashPolicy.createCheckpoint();
  }

  /** Delete old checkpoint(s). */
  public void expunge() throws IOException {
    trashPolicy.deleteCheckpoint();
  }

  /** get the current working directory */
  public Path getCurrentTrashDir() {
    return trashPolicy.getCurrentTrashDir();
  }

  /** get the configured trash policy */
  TrashPolicy getTrashPolicy() {
    return trashPolicy;
  }

  /** Return a {@link Runnable} that periodically empties the trash of all
   * users, intended to be run by the superuser.
   */
  public Runnable getEmptier() throws IOException {
    return trashPolicy.getEmptier();
  }
}
