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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * This interface is used for implementing different Trash policies.
 * Provides factory method to create instances of the configured Trash policy.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TrashPolicy extends Configured {

  private static final Logger LOG =
      LoggerFactory.getLogger(TrashPolicy.class);

  /**
   * Global configuration key for the Trash policy classname: {@value}.
   */
  public static final String FS_TRASH_CLASSNAME = "fs.trash.classname";

  /**
   * FS specific configuration key for the Trash policy classname: {@value}.
   */
  public static final String FS_TRASH_SCHEMA_CLASSNAME = "fs.%s.trash.classname";

  /**
   * The FileSystem.
   */
  protected FileSystem fs;

  /**
   * The path to trash directory.
   */
  protected Path trash;

  /**
   * The deletion interval for Emptier.
   */
  protected long deletionInterval;

  /**
   * Used to setup the trash policy. Must be implemented by all TrashPolicy
   * implementations.
   * @param conf the configuration to be used
   * @param fs the filesystem to be used
   * @param home the home directory
   * @deprecated Use {@link #initialize(Configuration, FileSystem)} instead.
   */
  @Deprecated
  public abstract void initialize(Configuration conf, FileSystem fs, Path home);

  /**
   * Used to setup the trash policy. Must be implemented by all TrashPolicy
   * implementations. Different from initialize(conf, fs, home), this one does
   * not assume trash always under /user/$USER due to HDFS encryption zone.
   * @param conf the configuration to be used
   * @param fs the filesystem to be used
   */
  public void initialize(Configuration conf, FileSystem fs) {
    throw new UnsupportedOperationException("TrashPolicy");
  }

  /**
   * Returns whether the Trash Policy is enabled for this filesystem.
   *
   * @return if isEnabled true,not false.
   */
  public abstract boolean isEnabled();

  /** 
   * Move a file or directory to the current trash directory.
   * @param path the path.
   * @return false if the item is already in the trash or trash is disabled
   * @throws IOException raised on errors performing I/O.
   */ 
  public abstract boolean moveToTrash(Path path) throws IOException;

  /** 
   * Create a trash checkpoint.
   * @throws IOException raised on errors performing I/O.
   */
  public abstract void createCheckpoint() throws IOException;

  /** 
   * Delete old trash checkpoint(s).
   * @throws IOException raised on errors performing I/O.
   */
  public abstract void deleteCheckpoint() throws IOException;

  /**
   * Delete all checkpoints immediately, ie empty trash.
   * @throws IOException raised on errors performing I/O.
   */
  public abstract void deleteCheckpointsImmediately() throws IOException;

  /**
   * Get the current working directory of the Trash Policy
   * This API does not work with files deleted from encryption zone when HDFS
   * data encryption at rest feature is enabled as rename file between
   * encryption zones or encryption zone and non-encryption zone is not allowed.
   *
   * The caller is recommend to use the new API
   * TrashPolicy#getCurrentTrashDir(Path path).
   * It returns the trash location correctly for the path specified no matter
   * the path is in encryption zone or not.
   *
   * @return the path.
   */
  public abstract Path getCurrentTrashDir();

  /**
   * Get the current trash directory for path specified based on the Trash
   * Policy
   * @param path path to be deleted
   * @return current trash directory for the path to be deleted
   * @throws IOException raised on errors performing I/O.
   */
  public Path getCurrentTrashDir(Path path) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** 
   * Return a {@link Runnable} that periodically empties the trash of all
   * users, intended to be run by the superuser.
   *
   * @throws IOException raised on errors performing I/O.
   * @return Runnable.
   */
  public abstract Runnable getEmptier() throws IOException;

  /**
   * Get the filesystem
   * @return the FS
   */
  public FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Get the path to trash directory.
   * @return The path to trash directory.
   */
  public Path getTrash() {
    return trash;
  }

  /**
   * The deletion interval for Emptier.
   * @return The deletion interval for Emptier.
   */
  public long getDeletionInterval() {
    return deletionInterval;
  }

  /**
   * Get an instance of the configured TrashPolicy based on the value
   * of the configuration parameter fs.trash.classname.
   *
   * @param conf the configuration to be used
   * @param fs the file system to be used
   * @param home the home directory
   * @return an instance of TrashPolicy
   * @deprecated Use {@link #getInstance(Configuration, FileSystem)} instead.
   */
  @Deprecated
  public static TrashPolicy getInstance(Configuration conf, FileSystem fs, Path home) {
    Class<? extends TrashPolicy> trashClass = conf.getClass(
        FS_TRASH_CLASSNAME, TrashPolicyDefault.class, TrashPolicy.class);
    TrashPolicy trash = ReflectionUtils.newInstance(trashClass, conf);
    trash.initialize(conf, fs, home); // initialize TrashPolicy
    return trash;
  }

  /**
   * Get an instance of the configured TrashPolicy based on the value of
   * the configuration parameter
   * <ol>
   *   <li>{@code fs.${fs.getUri().getScheme()}.trash.classname}</li>
   *   <li>{@code fs.trash.classname}</li>
   * </ol>
   * The configuration passed in is used to look up both values and load
   * in the policy class, not that of the FileSystem instance.
   * @param conf the configuration to be used for lookup and classloading
   * @param fs the file system to be used
   * @return an instance of TrashPolicy
   */
  @SuppressWarnings("ClassReferencesSubclass")
  public static TrashPolicy getInstance(Configuration conf, FileSystem fs) {
    String scheme = fs.getUri().getScheme();
    String key = String.format(FS_TRASH_SCHEMA_CLASSNAME, scheme);
    if (conf.get(key) == null) {
      // no specific trash policy for this scheme, use the default key
      key = FS_TRASH_CLASSNAME;
    }
    LOG.debug("Looking up trash policy from configuration key {}", key);
    Class<? extends TrashPolicy> trashClass = conf.getClass(
        key, TrashPolicyDefault.class, TrashPolicy.class);
    LOG.debug("Trash policy class: {}", trashClass);

    TrashPolicy trash = ReflectionUtils.newInstance(trashClass, conf);
    trash.initialize(conf, fs); // initialize TrashPolicy
    return trash;
  }
}
