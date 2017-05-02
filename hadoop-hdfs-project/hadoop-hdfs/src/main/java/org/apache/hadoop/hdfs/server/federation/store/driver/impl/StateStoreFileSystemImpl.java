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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateStoreDriver} implementation based on a filesystem. The most common uses
 * HDFS as a backend.
 */
public class StateStoreFileSystemImpl extends StateStoreFileBaseImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreFileSystemImpl.class);


  /** Configuration keys. */
  public static final String FEDERATION_STORE_FS_PATH =
      DFSConfigKeys.FEDERATION_STORE_PREFIX + "driver.fs.path";

  /** File system to back the State Store. */
  private FileSystem fs;
  /** Working path in the filesystem. */
  private String workPath;

  @Override
  protected boolean exists(String path) {
    try {
      return fs.exists(new Path(path));
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  protected boolean mkdir(String path) {
    try {
      return fs.mkdirs(new Path(path));
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  protected String getRootDir() {
    if (this.workPath == null) {
      String rootPath = getConf().get(FEDERATION_STORE_FS_PATH);
      URI workUri;
      try {
        workUri = new URI(rootPath);
        fs = FileSystem.get(workUri, getConf());
      } catch (Exception ex) {
        return null;
      }
      this.workPath = rootPath;
    }
    return this.workPath;
  }

  @Override
  public void close() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }

  /**
   * Get the folder path for the record class' data.
   *
   * @param cls Data record class.
   * @return Path of the folder containing the record class' data files.
   */
  private Path getPathForClass(Class<? extends BaseRecord> clazz) {
    if (clazz == null) {
      return null;
    }
    // TODO extract table name from class: entry.getTableName()
    String className = StateStoreUtils.getRecordName(clazz);
    return new Path(workPath, className);
  }

  @Override
  protected <T extends BaseRecord> void lockRecordRead(Class<T> clazz) {
    // Not required, synced with HDFS leasing
  }

  @Override
  protected <T extends BaseRecord> void unlockRecordRead(Class<T> clazz) {
    // Not required, synced with HDFS leasing
  }

  @Override
  protected <T extends BaseRecord> void lockRecordWrite(Class<T> clazz) {
    // TODO -> wait for lease to be available
  }

  @Override
  protected <T extends BaseRecord> void unlockRecordWrite(Class<T> clazz) {
    // TODO -> ensure lease is closed for the file
  }

  @Override
  protected <T extends BaseRecord> BufferedReader getReader(
      Class<T> clazz, String sub) {

    Path path = getPathForClass(clazz);
    if (sub != null && sub.length() > 0) {
      path = Path.mergePaths(path, new Path("/" + sub));
    }
    path = Path.mergePaths(path, new Path("/" + getDataFileName()));

    try {
      FSDataInputStream fdis = fs.open(path);
      InputStreamReader isr =
          new InputStreamReader(fdis, StandardCharsets.UTF_8);
      BufferedReader reader = new BufferedReader(isr);
      return reader;
    } catch (IOException ex) {
      LOG.error("Cannot open write stream for {}  to {}",
          clazz.getSimpleName(), path);
      return null;
    }
  }

  @Override
  protected <T extends BaseRecord> BufferedWriter getWriter(
      Class<T> clazz, String sub) {

    Path path = getPathForClass(clazz);
    if (sub != null && sub.length() > 0) {
      path = Path.mergePaths(path, new Path("/" + sub));
    }
    path = Path.mergePaths(path, new Path("/" + getDataFileName()));

    try {
      FSDataOutputStream fdos = fs.create(path, true);
      OutputStreamWriter osw =
          new OutputStreamWriter(fdos, StandardCharsets.UTF_8);
      BufferedWriter writer = new BufferedWriter(osw);
      return writer;
    } catch (IOException ex) {
      LOG.error("Cannot open write stream for {} to {}",
          clazz.getSimpleName(), path);
      return null;
    }
  }
}