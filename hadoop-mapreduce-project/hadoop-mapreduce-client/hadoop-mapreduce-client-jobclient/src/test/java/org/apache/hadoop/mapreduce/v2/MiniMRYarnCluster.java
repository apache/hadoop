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

package org.apache.hadoop.mapreduce.v2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;

/**
 * Configures and starts the MR-specific components in the YARN cluster.
 *
 */
public class MiniMRYarnCluster extends MiniYARNCluster {

  public static final String APPJAR =
    "../hadoop-mapreduce-client-app/target/"
        + MRConstants.HADOOP_MAPREDUCE_CLIENT_APP_JAR_NAME;

  private static final Log LOG = LogFactory.getLog(MiniMRYarnCluster.class);
  private JobHistoryServer historyServer;
  private JobHistoryServerWrapper historyServerWrapper;

  public MiniMRYarnCluster(String testName) {
    super(testName);
    //TODO: add the history server
    historyServerWrapper = new JobHistoryServerWrapper();
    addService(historyServerWrapper);
  }

  @Override
  public void init(Configuration conf) {
    conf.set(MRConfig.FRAMEWORK_NAME, "yarn");
    conf.set(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    conf.set(MRConstants.APPS_STAGING_DIR_KEY, new File(getTestWorkDir(),
        "apps_staging_dir/${user.name}/").getAbsolutePath());
    conf.set(MRConfig.MASTER_ADDRESS, "test"); // The default is local because of
                                             // which shuffle doesn't happen
    //configure the shuffle service in NM
    conf.setStrings(AuxServices.AUX_SERVICES,
        new String[] { ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID });
    conf.setClass(String.format(AuxServices.AUX_SERVICE_CLASS_FMT,
        ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID), ShuffleHandler.class,
        Service.class);
    // Non-standard shuffle port
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 8083);
    conf.setClass(NMConfig.NM_CONTAINER_EXECUTOR_CLASS,
        DefaultContainerExecutor.class, ContainerExecutor.class);

    // TestMRJobs is for testing non-uberized operation only; see TestUberAM
    // for corresponding uberized tests.
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);

    super.init(conf);
  }

  private class JobHistoryServerWrapper extends AbstractService {
    public JobHistoryServerWrapper() {
      super(JobHistoryServerWrapper.class.getName());
    }

    @Override
    public synchronized void start() {
      try {
        historyServer = new JobHistoryServer();
        historyServer.init(getConfig());
        new Thread() {
          public void run() {
            historyServer.start();
          };
        }.start();
        while (historyServer.getServiceState() == STATE.INITED) {
          LOG.info("Waiting for HistoryServer to start...");
          Thread.sleep(1500);
        }
        if (historyServer.getServiceState() != STATE.STARTED) {
          throw new IOException("HistoryServer failed to start");
        }
        super.start();
      } catch (Throwable t) {
        throw new YarnException(t);
      }
    }

    @Override
    public synchronized void stop() {
      if (historyServer != null) {
        historyServer.stop();
      }
      super.stop();
    }
  }

  public JobHistoryServer getHistoryServer() {
    return this.historyServer;
  }
}
