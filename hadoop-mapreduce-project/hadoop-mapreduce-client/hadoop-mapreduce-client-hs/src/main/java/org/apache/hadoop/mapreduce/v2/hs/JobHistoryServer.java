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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.CompositeService;

/******************************************************************
 * {@link JobHistoryServer} is responsible for servicing all job history
 * related requests from client.
 *
 *****************************************************************/
public class JobHistoryServer extends CompositeService {
  private static final Log LOG = LogFactory.getLog(JobHistoryServer.class);
  private HistoryContext historyContext;
  private HistoryClientService clientService;
  private JobHistory jobHistoryService;

  public JobHistoryServer() {
    super(JobHistoryServer.class.getName());
  }

  @Override
  public synchronized void init(Configuration conf) {
    Configuration config = new YarnConfiguration(conf);
    try {
      doSecureLogin(conf);
    } catch(IOException ie) {
      throw new YarnException("History Server Failed to login", ie);
    }
    jobHistoryService = new JobHistory();
    historyContext = (HistoryContext)jobHistoryService;
    clientService = new HistoryClientService(historyContext);
    addService(jobHistoryService);
    addService(clientService);
    super.init(config);
  }

  protected void doSecureLogin(Configuration conf) throws IOException {
    SecurityUtil.login(conf, JHAdminConfig.MR_HISTORY_KEYTAB,
        JHAdminConfig.MR_HISTORY_PRINCIPAL);
  }

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(JobHistoryServer.class, args, LOG);
    try {
      JobHistoryServer jobHistoryServer = new JobHistoryServer();
      Runtime.getRuntime().addShutdownHook(
          new CompositeServiceShutdownHook(jobHistoryServer));
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      jobHistoryServer.init(conf);
      jobHistoryServer.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting JobHistoryServer", t);
      System.exit(-1);
    }
  }
}
