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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StateStoreDriver} implementation based on a filesystem. The common
 * implementation uses HDFS as a backend. The path can be specified setting
 * dfs.federation.router.store.driver.fs.path=hdfs://host:port/path/to/store.
 */
public class StateStoreFileSystemImpl extends StateStoreFileBaseImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreFileSystemImpl.class);

  /** Configuration keys. */
  public static final String FEDERATION_STORE_FS_PATH =
      RBFConfigKeys.FEDERATION_STORE_PREFIX + "driver.fs.path";

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
  protected boolean rename(String src, String dst) {
    try {
      FileUtil.rename(fs, new Path(src), new Path(dst), Options.Rename.OVERWRITE);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot rename {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  protected boolean remove(String path) {
    try {
      return fs.delete(new Path(path), true);
    } catch (Exception e) {
      LOG.error("Cannot remove {}", path, e);
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
  protected int getConcurrentFilesAccessNumThreads() {
    return getConf().getInt(RBFConfigKeys.FEDERATION_STORE_FS_ASYNC_THREADS,
        RBFConfigKeys.FEDERATION_STORE_FS_ASYNC_THREADS_DEFAULT);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (fs != null) {
      fs.close();
    }
  }

  @Override
  protected <T extends BaseRecord> BufferedReader getReader(String pathName) {
    BufferedReader reader = null;
    Path path = new Path(pathName);
    try {
      FSDataInputStream fdis = fs.open(path);
      InputStreamReader isr =
          new InputStreamReader(fdis, StandardCharsets.UTF_8);
      reader = new BufferedReader(isr);
    } catch (IOException ex) {
      LOG.error("Cannot open read stream for {}", path, ex);
    }
    return reader;
  }

  @Override
  @VisibleForTesting
  public <T extends BaseRecord> BufferedWriter getWriter(String pathName) {
    BufferedWriter writer = null;
    Path path = new Path(pathName);
    try {
      FSDataOutputStream fdos = fs.create(path, true);
      OutputStreamWriter osw =
          new OutputStreamWriter(fdos, StandardCharsets.UTF_8);
      writer = new BufferedWriter(osw);
    } catch (IOException ex) {
      LOG.error("Cannot open write stream for {}", path, ex);
    }
    return writer;
  }

  @Override
  protected List<String> getChildren(String pathName) {
    Path path = new Path(workPath, pathName);
    try {
      FileStatus[] files = fs.listStatus(path);
      List<String> ret = new ArrayList<>(files.length);
      for (FileStatus file : files) {
        Path filePath = file.getPath();
        String fileName = filePath.getName();
        ret.add(fileName);
      }
      return ret;
    } catch (Exception e) {
      LOG.error("Cannot get children for {}", pathName, e);
      return Collections.emptyList();
    }
  }
}
