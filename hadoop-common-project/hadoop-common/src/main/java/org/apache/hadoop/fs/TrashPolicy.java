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

/** 
 * This interface is used for implementing different Trash policies.
 * Provides factory method to create instances of the configured Trash policy.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TrashPolicy extends Configured {
  protected FileSystem fs; // the FileSystem
  protected Path trash; // path to trash directory
  protected long deletionInterval; // deletion interval for Emptier

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
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the Trash Policy is enabled for this filesystem.
   */
  public abstract boolean isEnabled();

  /** 
   * Move a file or directory to the current trash directory.
   * @return false if the item is already in the trash or trash is disabled
   */ 
  public abstract boolean moveToTrash(Path path) throws IOException;

  /** 
   * Create a trash checkpoint. 
   */
  public abstract void createCheckpoint() throws IOException;

  /** 
   * Delete old trash checkpoint(s).
   */
  public abstract void deleteCheckpoint() throws IOException;

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
   */
  public abstract Path getCurrentTrashDir();

  /**
   * Get the current trash directory for path specified based on the Trash
   * Policy
   * @param path path to be deleted
   * @return current trash directory for the path to be deleted
   * @throws IOException
   */
  public Path getCurrentTrashDir(Path path) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** 
   * Return a {@link Runnable} that periodically empties the trash of all
   * users, intended to be run by the superuser.
   */
  public abstract Runnable getEmptier() throws IOException;

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
        "fs.trash.classname", TrashPolicyDefault.class, TrashPolicy.class);
    TrashPolicy trash = ReflectionUtils.newInstance(trashClass, conf);
    trash.initialize(conf, fs, home); // initialize TrashPolicy
    return trash;
  }

  /**
   * Get an instance of the configured TrashPolicy based on the value
   * of the configuration parameter fs.trash.classname.
   *
   * @param conf the configuration to be used
   * @param fs the file system to be used
   * @return an instance of TrashPolicy
   */
  public static TrashPolicy getInstance(Configuration conf, FileSystem fs) {
    Class<? extends TrashPolicy> trashClass = conf.getClass(
        "fs.trash.classname", TrashPolicyDefault.class, TrashPolicy.class);
    TrashPolicy trash = ReflectionUtils.newInstance(trashClass, conf);
    trash.initialize(conf, fs); // initialize TrashPolicy
    return trash;
  }
}
