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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

@InterfaceAudience.Private
public class DatanodeJspHelper {
  private static DFSClient getDFSClient(final UserGroupInformation user,
                                        final String addr,
                                        final Configuration conf
                                        ) throws IOException,
                                                 InterruptedException {
    return
      user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        @Override
        public DFSClient run() throws IOException {
          return new DFSClient(NetUtils.createSocketAddr(addr), conf);
        }
      });
  }
  
  /** Get DFSClient for a namenode corresponding to the BPID from a datanode */
  public static DFSClient getDFSClient(final HttpServletRequest request,
      final DataNode datanode, final Configuration conf,
      final UserGroupInformation ugi) throws IOException, InterruptedException {
    final String nnAddr = request.getParameter(JspHelper.NAMENODE_ADDRESS);
    return getDFSClient(ugi, nnAddr, conf);
  }
}
