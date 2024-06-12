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
package org.apache.hadoop.fs.compat.common;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Random;

public abstract class AbstractHdfsCompatCase {
  private static final Random RANDOM = new Random();

  private FileSystem fs;
  private HdfsCompatEnvironment env;
  private Path localPath;

  public AbstractHdfsCompatCase() {
  }

  public void init(HdfsCompatEnvironment environment) {
    this.env = environment;
    this.fs = env.getFileSystem();
    LocalFileSystem localFs = env.getLocalFileSystem();
    this.localPath = localFs.makeQualified(new Path(env.getLocalTmpDir()));
  }

  public FileSystem fs() {
    return fs;
  }

  public Path getRootPath() {
    return this.env.getRoot();
  }

  public Path getBasePath() {
    return this.env.getBase();
  }

  public Path getUniquePath() {
    return getUniquePath(getBasePath());
  }

  public static Path getUniquePath(Path basePath) {
    return new Path(basePath, System.currentTimeMillis()
        + "_" + RANDOM.nextLong());
  }

  public Path makePath(String name) {
    return new Path(getUniquePath(), name);
  }

  public Path getLocalPath() {
    return localPath;
  }

  public String getPrivilegedUser() {
    return this.env.getPrivilegedUser();
  }

  public String[] getStoragePolicyNames() {
    return this.env.getStoragePolicyNames();
  }

  public String getDelegationTokenRenewer() {
    return this.env.getDelegationTokenRenewer();
  }
}