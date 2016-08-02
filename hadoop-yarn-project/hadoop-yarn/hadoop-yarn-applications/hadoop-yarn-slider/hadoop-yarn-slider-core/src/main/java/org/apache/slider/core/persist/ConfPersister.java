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

package org.apache.slider.core.persist;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.slider.common.tools.CoreFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

/**
 * Class to implement persistence of a configuration.
 *
 * This code contains the logic to acquire and release locks.
 * # writelock MUST be acquired exclusively for writes. This is done
 * by creating the file with no overwrite
 * # shared readlock MUST be acquired for reads. This is done by creating the readlock
 * file with overwrite forbidden -but treating a failure as a sign that
 * the lock exists, and therefore the operation can continue.
 * # releaselock is only released if the client created it.
 * # after acquiring either lock, client must check for the alternate lock
 * existing. If it is, release lock and fail.
 * 
 * There's one small race here: multiple readers; first reader releases lock
 * while second is in use. 
 * 
 * Strict Fix: client checks for readlock after read completed.
 * If it is not there, problem: fail. But this massively increases the risk of
 * false negatives.
 * 
 * This isn't 100% perfect, because of the condition where the owner releases
 * a lock, a writer grabs its lock & writes to it, the reader gets slightly
 * contaminated data:
 * own-share-delete-write-own-release(shared)-delete
 * 
 * We are assuming that the rate of change is low enough that this is rare, and
 * of limited damage.
 * 
 * ONCE A CLUSTER IS RUNNING, ONLY THE AM MAY PERSIST UPDATES VIA ITS APIs
 * 
 * That is: outside the AM, a writelock MUST only be acquired after verifying there is no
 * running application.
 */
public class ConfPersister {
  private static final Logger log =
    LoggerFactory.getLogger(ConfPersister.class);


  private final ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();

  private final CoreFileSystem coreFS;
  private final FileSystem fileSystem;
  private final Path persistDir;
  private final Path internal, resources, app_conf;
  private final Path writelock, readlock;

  public ConfPersister(CoreFileSystem coreFS, Path persistDir) {
    this.coreFS = coreFS;
    this.persistDir = persistDir;
    internal = new Path(persistDir, Filenames.INTERNAL);
    resources = new Path(persistDir, Filenames.RESOURCES);
    app_conf = new Path(persistDir, Filenames.APPCONF);
    writelock = new Path(persistDir, Filenames.WRITELOCK);
    readlock = new Path(persistDir, Filenames.READLOCK);
    fileSystem = coreFS.getFileSystem();
  }

  /**
   * Get the target directory
   * @return the directory for persistence
   */
  public Path getPersistDir() {
    return persistDir;
  }

  /**
   * Make the persistent directory
   * @throws IOException IO failure
   */
  public void mkPersistDir() throws IOException {
    coreFS.getFileSystem().mkdirs(persistDir);
  }
  
  @Override
  public String toString() {
    return "Persister to " + persistDir;
  }

  /**
   * Acquire the writelock
   * @throws IOException IO
   * @throws LockAcquireFailedException
   */
  @VisibleForTesting
  void acquireWritelock() throws IOException,
                                 LockAcquireFailedException {
    mkPersistDir();
    long now = System.currentTimeMillis();
    try {
      coreFS.cat(writelock, false, new Date(now).toGMTString());
    } catch (FileAlreadyExistsException e) {
      // filesystems should raise this (HDFS does)
      throw new LockAcquireFailedException(writelock);
    } catch (IOException e) {
      // some filesystems throw a generic IOE
      throw new LockAcquireFailedException(writelock, e);
    }
    //here the lock is acquired, but verify there is no readlock
    boolean lockFailure;
    try {
      lockFailure = readLockExists();
    } catch (IOException e) {
      lockFailure = true;
    }
    if (lockFailure) {
      releaseWritelock();
      throw new LockAcquireFailedException(readlock);
    }
  }

  @VisibleForTesting
  boolean readLockExists() throws IOException {
    return fileSystem.exists(readlock);
  }

  /**
   * Release the writelock if it is present.
   * IOExceptions are logged
   */
  @VisibleForTesting
  boolean releaseWritelock() {
    try {
      return fileSystem.delete(writelock, false);
    } catch (IOException e) {
      log.warn("IOException releasing writelock {} ", writelock, e);
    }
    return false;
  }
  
  /**
   * Acquire the writelock
   * @throws IOException IO
   * @throws LockAcquireFailedException
   * @throws FileNotFoundException if the target dir does not exist.
   */
  @VisibleForTesting
  boolean acquireReadLock() throws FileNotFoundException,
                                  IOException,
                                  LockAcquireFailedException {
    if (!coreFS.getFileSystem().exists(persistDir)) {
      // the dir is not there, so the data is not there, so there
      // is nothing to read
      throw new FileNotFoundException(persistDir.toString());
    }
    long now = System.currentTimeMillis();
    boolean owner;
    try {
      coreFS.cat(readlock, false, new Date(now).toGMTString());
      owner = true;
    } catch (IOException e) {
      owner = false;
    }
    //here the lock is acquired, but verify there is no readlock
    boolean lockFailure;
    try {
      lockFailure = writelockExists();
    } catch (IOException e) {
      lockFailure = true;
    }
    if (lockFailure) {
      releaseReadlock(owner);
      throw new LockAcquireFailedException(writelock);
    }
    return owner;
  }

  @VisibleForTesting
  boolean writelockExists() throws IOException {
    return fileSystem.exists(writelock);
  }

  /**
   * Release the writelock if it is present.
   * IOExceptions are downgraded to failures
   * @return true if the lock was present and then released  
   */
  @VisibleForTesting
  boolean releaseReadlock(boolean owner) {
    if (owner) {
      try {
        return fileSystem.delete(readlock, false);
      } catch (IOException e) {
        log.warn("IOException releasing writelock {} ", readlock, e);
      }
    }
    return false;
  }

  private void saveConf(AggregateConf conf) throws IOException {
    confTreeSerDeser.save(fileSystem, internal, conf.getInternal(), true);
    confTreeSerDeser.save(fileSystem, resources, conf.getResources(), true);
    confTreeSerDeser.save(fileSystem, app_conf, conf.getAppConf(), true);
  }

  private void loadConf(AggregateConf conf) throws IOException {
    conf.setInternal(confTreeSerDeser.load(fileSystem, internal));
    conf.setResources(confTreeSerDeser.load(fileSystem, resources));
    conf.setAppConf(confTreeSerDeser.load(fileSystem, app_conf));
  }


  private void maybeExecLockHeldAction(LockHeldAction action) throws
      IOException,
      SliderException {
    if (action != null) {
      action.execute();
    }
  }
  
  /**
   * Save the configuration
   * @param conf configuration to fill in
   * @param action
   * @throws IOException IO problems
   * @throws LockAcquireFailedException the lock could not be acquired
   */
  public void save(AggregateConf conf, LockHeldAction action) throws
      IOException,
      SliderException,
      LockAcquireFailedException {
    acquireWritelock();
    try {
      saveConf(conf);
      maybeExecLockHeldAction(action);
    } finally {
      releaseWritelock();
    }
  }

  /**
   * Load the configuration. If a lock failure is raised, the 
   * contents of the configuration MAY have changed -lock race conditions
   * are looked for on exit
   * @param conf configuration to fill in
   * @throws IOException IO problems
   * @throws LockAcquireFailedException the lock could not be acquired
   */
  public void load(AggregateConf conf) throws
      FileNotFoundException,
      IOException,
      SliderException,
      LockAcquireFailedException {
    boolean owner = acquireReadLock();
    try {
      loadConf(conf);
    } finally {
      releaseReadlock(owner);
    }
  }
  

}
