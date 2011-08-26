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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;

public class ClientCache {

  private final Configuration conf;
  private final ResourceMgrDelegate rm;
  
  private static final Log LOG = LogFactory.getLog(ClientCache.class);

  private Map<JobID, ClientServiceDelegate> cache = 
    new HashMap<JobID, ClientServiceDelegate>();
  
  private MRClientProtocol hsProxy;

  ClientCache(Configuration conf, ResourceMgrDelegate rm) {
    this.conf = conf;
    this.rm = rm;
  }

  //TODO: evict from the cache on some threshold
  synchronized ClientServiceDelegate getClient(JobID jobId) {
	if (hsProxy == null) {
      try {
		hsProxy = instantiateHistoryProxy();
	  } catch (IOException e) {
		LOG.warn("Could not connect to History server.", e);
		throw new YarnException("Could not connect to History server.", e);
	  }
	}
    ClientServiceDelegate client = cache.get(jobId);
    if (client == null) {
      client = new ClientServiceDelegate(conf, rm, jobId, hsProxy);
      cache.put(jobId, client);
    }
    return client;
  }

  private MRClientProtocol instantiateHistoryProxy()
  throws IOException {
	String serviceAddr = conf.get(JHConfig.HS_BIND_ADDRESS,
	          JHConfig.DEFAULT_HS_BIND_ADDRESS);
    LOG.info("Connecting to HistoryServer at: " + serviceAddr);
    Configuration myConf = new Configuration(conf);
    //TODO This should ideally be using it's own class (instead of ClientRMSecurityInfo)
    myConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    YarnRPC rpc = YarnRPC.create(myConf);
    LOG.info("Connected to HistoryServer at: " + serviceAddr);
    return (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
        NetUtils.createSocketAddr(serviceAddr), myConf);
  }
}
