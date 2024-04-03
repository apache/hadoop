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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class HdfsCompatEnvironment {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatEnvironment.class);
  private static final String DATE_FORMAT = "yyyy_MM_dd_HH_mm_ss";
  private static final Random RANDOM = new Random();
  private final Path uri;
  private final Configuration conf;
  private FileSystem fs;
  private LocalFileSystem localFs;
  private Path rootDir;
  private Path baseDir;
  private String defaultLocalDir;
  private String[] defaultStoragePolicyNames;

  public HdfsCompatEnvironment(Path uri, Configuration conf) {
    this.conf = conf;
    this.uri = uri;
  }

  public void init() throws IOException {
    Date now = new Date();
    String uuid = UUID.randomUUID().toString();
    String uniqueDir = "hadoop-compatibility-benchmark/" +
        new SimpleDateFormat(DATE_FORMAT).format(now) + "/" + uuid;

    this.fs = uri.getFileSystem(conf);
    this.localFs = FileSystem.getLocal(conf);
    this.rootDir = fs.makeQualified(new Path("/"));
    this.baseDir = fs.makeQualified(new Path(uri, uniqueDir));
    String tmpdir = getEnvTmpDir();
    if ((tmpdir == null) || tmpdir.isEmpty()) {
      LOG.warn("Cannot get valid io.tmpdir, will use /tmp");
      tmpdir = "/tmp";
    }
    this.defaultLocalDir = new File(tmpdir, uniqueDir).getAbsolutePath();
    this.defaultStoragePolicyNames = getDefaultStoragePolicyNames();
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public LocalFileSystem getLocalFileSystem() {
    return localFs;
  }

  public Path getRoot() {
    return rootDir;
  }

  public Path getBase() {
    return baseDir;
  }

  public String getLocalTmpDir() {
    final String scheme = this.uri.toUri().getScheme();
    final String key = "fs." + scheme + ".compatibility.local.tmpdir";
    final String localDir = conf.get(key, null);
    return (localDir != null) ? localDir : defaultLocalDir;
  }

  public String getPrivilegedUser() {
    final String scheme = this.uri.toUri().getScheme();
    final String key = "fs." + scheme + ".compatibility.privileged.user";
    final String privileged = conf.get(key, null);
    return (privileged != null) ? privileged :
        conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
            DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
  }

  public String[] getStoragePolicyNames() {
    final String scheme = this.uri.toUri().getScheme();
    final String key = "fs." + scheme + ".compatibility.storage.policies";
    final String storagePolicies = conf.get(key, null);
    return (storagePolicies != null) ? storagePolicies.split(",") :
        defaultStoragePolicyNames.clone();
  }

  public String getDelegationTokenRenewer() {
    final String scheme = this.uri.toUri().getScheme();
    final String key = "fs." + scheme + ".compatibility.delegation.token.renewer";
    return conf.get(key, "");
  }

  private String getEnvTmpDir() {
    final String systemDefault = System.getProperty("java.io.tmpdir");
    if ((systemDefault == null) || systemDefault.isEmpty()) {
      return null;
    }
    String[] tmpDirs = systemDefault.split(",|" + File.pathSeparator);
    List<String> validDirs = Arrays.stream(tmpDirs).filter(
        s -> (s != null && !s.isEmpty())
    ).collect(Collectors.toList());
    if (validDirs.isEmpty()) {
      return null;
    }
    final String tmpDir = validDirs.get(
        RANDOM.nextInt(validDirs.size()));
    return new File(tmpDir).getAbsolutePath();
  }

  private String[] getDefaultStoragePolicyNames() {
    Collection<? extends BlockStoragePolicySpi> policies = null;
    try {
      policies = fs.getAllStoragePolicies();
    } catch (Exception e) {
      LOG.warn("Cannot get storage policy", e);
    }
    if ((policies == null) || policies.isEmpty()) {
      return new String[]{"Hot"};
    } else {
      return policies.stream().map(BlockStoragePolicySpi::getName).toArray(String[]::new);
    }
  }
}