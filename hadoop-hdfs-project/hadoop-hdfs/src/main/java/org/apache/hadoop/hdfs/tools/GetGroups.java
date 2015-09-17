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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.tools.GetGroupsBase;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFS implementation of a tool for getting the groups which a given user
 * belongs to.
 */
@InterfaceAudience.Private
public class GetGroups extends GetGroupsBase {
  
  private static final Log LOG = LogFactory.getLog(GetGroups.class);
  
  static final String USAGE = "Usage: hdfs groups [username ...]";

  static{
    HdfsConfiguration.init();
  }

  
  public GetGroups(Configuration conf) {
    super(conf);
  }

  public GetGroups(Configuration conf, PrintStream out) {
    super(conf, out);
  }
  
  @Override
  protected InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return DFSUtilClient.getNNAddress(conf);
  }
  
  @Override
  public void setConf(Configuration conf) {
    conf = new HdfsConfiguration(conf);
    String nameNodePrincipal = conf.get(
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "");
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using NN principal: " + nameNodePrincipal);
    }

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        nameNodePrincipal);
    
    super.setConf(conf);
  }
  
  @Override
  protected GetUserMappingsProtocol getUgmProtocol() throws IOException {
    return NameNodeProxies.createProxy(getConf(), FileSystem.getDefaultUri(getConf()),
        GetUserMappingsProtocol.class).getProxy();
  }

  public static void main(String[] argv) throws Exception {
    if (DFSUtil.parseHelpArgument(argv, USAGE, System.out, true)) {
      System.exit(0);
    }
    
    int res = ToolRunner.run(new GetGroups(new HdfsConfiguration()), argv);
    System.exit(res);
  }
}