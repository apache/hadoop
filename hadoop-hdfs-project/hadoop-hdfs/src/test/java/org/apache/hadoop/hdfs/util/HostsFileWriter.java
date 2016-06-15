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

package org.apache.hadoop.hdfs.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager;

import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;

import static org.junit.Assert.assertTrue;

public class HostsFileWriter {
  private FileSystem localFileSys;
  private Path fullDir;
  private Path excludeFile;
  private Path includeFile;
  private Path combinedFile;
  private boolean isLegacyHostsFile = false;

  public void initialize(Configuration conf, String dir) throws IOException {
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = new Path(MiniDFSCluster.getBaseDirectory());
    this.fullDir = new Path(workingDir, dir);
    assertTrue(localFileSys.mkdirs(this.fullDir));

    if (conf.getClass(
        DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
            HostFileManager.class, HostConfigManager.class).equals(
                HostFileManager.class)) {
      isLegacyHostsFile = true;
    }
    if (isLegacyHostsFile) {
      excludeFile = new Path(fullDir, "exclude");
      includeFile = new Path(fullDir, "include");
      DFSTestUtil.writeFile(localFileSys, excludeFile, "");
      DFSTestUtil.writeFile(localFileSys, includeFile, "");
      conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
      conf.set(DFSConfigKeys.DFS_HOSTS, includeFile.toUri().getPath());
    } else {
      combinedFile = new Path(fullDir, "all");
      conf.set(DFSConfigKeys.DFS_HOSTS, combinedFile.toString());
    }
  }

  public void initExcludeHost(String hostNameAndPort) throws IOException {
    initExcludeHosts(hostNameAndPort);
  }

  public void initExcludeHosts(String... hostNameAndPorts) throws IOException {
    StringBuilder excludeHosts = new StringBuilder();
    if (isLegacyHostsFile) {
      for (String hostNameAndPort : hostNameAndPorts) {
        excludeHosts.append(hostNameAndPort).append("\n");
      }
      DFSTestUtil.writeFile(localFileSys, excludeFile, excludeHosts.toString());
    } else {
      HashSet<DatanodeAdminProperties> allDNs = new HashSet<>();
      for (String hostNameAndPort : hostNameAndPorts) {
        DatanodeAdminProperties dn = new DatanodeAdminProperties();
        String[] hostAndPort = hostNameAndPort.split(":");
        dn.setHostName(hostAndPort[0]);
        dn.setPort(Integer.parseInt(hostAndPort[1]));
        dn.setAdminState(AdminStates.DECOMMISSIONED);
        allDNs.add(dn);
      }
      CombinedHostsFileWriter.writeFile(combinedFile.toString(), allDNs);
    }
  }

  public void initIncludeHosts(String[] hostNameAndPorts) throws IOException {
    StringBuilder includeHosts = new StringBuilder();
    if (isLegacyHostsFile) {
      for(String hostNameAndPort : hostNameAndPorts) {
        includeHosts.append(hostNameAndPort).append("\n");
      }
      DFSTestUtil.writeFile(localFileSys, includeFile,
          includeHosts.toString());
    } else {
      HashSet<DatanodeAdminProperties> allDNs = new HashSet<>();
      for(String hostNameAndPort : hostNameAndPorts) {
        String[] hostAndPort = hostNameAndPort.split(":");
        DatanodeAdminProperties dn = new DatanodeAdminProperties();
        dn.setHostName(hostAndPort[0]);
        dn.setPort(Integer.parseInt(hostAndPort[1]));
        allDNs.add(dn);
      }
      CombinedHostsFileWriter.writeFile(combinedFile.toString(), allDNs);
    }
  }

  public void initIncludeHosts(DatanodeAdminProperties[] datanodes)
      throws IOException {
    CombinedHostsFileWriter.writeFile(combinedFile.toString(),
        new HashSet<>(Arrays.asList(datanodes)));
  }

  public void cleanup() throws IOException {
    if (localFileSys.exists(fullDir)) {
      FileUtils.deleteQuietly(new File(fullDir.toUri().getPath()));
    }
  }

  public Path getIncludeFile() {
    return includeFile;
  }

  public Path getExcludeFile() {
    return excludeFile;
  }
}
