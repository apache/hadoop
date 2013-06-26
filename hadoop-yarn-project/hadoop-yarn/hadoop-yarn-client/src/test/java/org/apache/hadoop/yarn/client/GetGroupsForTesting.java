/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.GetGroupsBase;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;

public class GetGroupsForTesting extends GetGroupsBase {
  
  public GetGroupsForTesting(Configuration conf) {
    super(conf);
  }

  public GetGroupsForTesting(Configuration conf, PrintStream out) {
    super(conf, out);
  }
  
  @Override
  protected InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return conf.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
  }
  
  @Override
  public void setConf(Configuration conf) {
    conf = new YarnConfiguration(conf);
    super.setConf(conf);
  }
  
  @Override
  protected GetUserMappingsProtocol getUgmProtocol() throws IOException {
    Configuration conf = getConf();
    
    final InetSocketAddress addr = conf.getSocketAddr(
        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    final YarnRPC rpc = YarnRPC.create(conf);
    
    ResourceManagerAdministrationProtocol adminProtocol = (ResourceManagerAdministrationProtocol) rpc.getProxy(
        ResourceManagerAdministrationProtocol.class, addr, getConf());

    return adminProtocol;
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new GetGroupsForTesting(new YarnConfiguration()), argv);
    System.exit(res);
  }

}
