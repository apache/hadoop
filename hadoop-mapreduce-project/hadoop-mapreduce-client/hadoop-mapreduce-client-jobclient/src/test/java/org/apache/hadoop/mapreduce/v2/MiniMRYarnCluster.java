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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * Configures and starts the MR-specific components in the YARN cluster.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MiniMRYarnCluster extends MiniYARNCluster {

  public static final String APPJAR = JarFinder.getJar(LocalContainerLauncher.class);

  private static final Log LOG = LogFactory.getLog(MiniMRYarnCluster.class);
  private JobHistoryServer historyServer;
  private JobHistoryServerWrapper historyServerWrapper;

  public MiniMRYarnCluster(String testName) {
    this(testName, 1);
  }

  public MiniMRYarnCluster(String testName, int noOfNMs) {
    this(testName, noOfNMs, false);
  }
  @Deprecated
  public MiniMRYarnCluster(String testName, int noOfNMs, boolean enableAHS) {
    super(testName, 1, noOfNMs, 4, 4, enableAHS);
    historyServerWrapper = new JobHistoryServerWrapper();
    addService(historyServerWrapper);
  }

  public static String getResolvedMRHistoryWebAppURLWithoutScheme(
      Configuration conf, boolean isSSLEnabled) {
    InetSocketAddress address = null;
    if (isSSLEnabled) {
      address =
          conf.getSocketAddr(JHAdminConfig.MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
              JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS,
              JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT);
    } else {
      address =
          conf.getSocketAddr(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
              JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS,
              JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT);    }
    address = NetUtils.getConnectAddress(address);
    StringBuffer sb = new StringBuffer();
    InetAddress resolved = address.getAddress();
    if (resolved == null || resolved.isAnyLocalAddress() || 
        resolved.isLoopbackAddress()) {
      String lh = address.getHostName();
      try {
        lh = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        //Ignore and fallback.
      }
      sb.append(lh);
    } else {
      sb.append(address.getHostName());
    }
    sb.append(":").append(address.getPort());
    return sb.toString();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    if (conf.get(MRJobConfig.MR_AM_STAGING_DIR) == null) {
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, new File(getTestWorkDir(),
          "apps_staging_dir/").getAbsolutePath());
    }

    // By default, VMEM monitoring disabled, PMEM monitoring enabled.
    if (!conf.getBoolean(
        MRConfig.MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING,
        MRConfig.DEFAULT_MAPREDUCE_MINICLUSTER_CONTROL_RESOURCE_MONITORING)) {
      conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
      conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    }

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,  "000");

    try {
      Path stagingPath = FileContext.getFileContext(conf).makeQualified(
          new Path(conf.get(MRJobConfig.MR_AM_STAGING_DIR)));
      /*
       * Re-configure the staging path on Windows if the file system is localFs.
       * We need to use a absolute path that contains the drive letter. The unit
       * test could run on a different drive than the AM. We can run into the
       * issue that job files are localized to the drive where the test runs on,
       * while the AM starts on a different drive and fails to find the job
       * metafiles. Using absolute path can avoid this ambiguity.
       */
      if (Path.WINDOWS) {
        if (LocalFileSystem.class.isInstance(stagingPath.getFileSystem(conf))) {
          conf.set(MRJobConfig.MR_AM_STAGING_DIR,
              new File(conf.get(MRJobConfig.MR_AM_STAGING_DIR))
                  .getAbsolutePath());
        }
      }
      FileContext fc=FileContext.getFileContext(stagingPath.toUri(), conf);
      if (fc.util().exists(stagingPath)) {
        LOG.info(stagingPath + " exists! deleting...");
        fc.delete(stagingPath, true);
      }
      LOG.info("mkdir: " + stagingPath);
      //mkdir the staging directory so that right permissions are set while running as proxy user
      fc.mkdir(stagingPath, null, true);
      //mkdir done directory as well 
      String doneDir = JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
      Path doneDirPath = fc.makeQualified(new Path(doneDir));
      fc.mkdir(doneDirPath, null, true);
    } catch (IOException e) {
      throw new YarnRuntimeException("Could not create staging directory. ", e);
    }
    conf.set(MRConfig.MASTER_ADDRESS, "test"); // The default is local because of
                                             // which shuffle doesn't happen
    //configure the shuffle service in NM
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] { ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
        ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID), ShuffleHandler.class,
        Service.class);

    // Non-standard shuffle port
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);

    conf.setClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        DefaultContainerExecutor.class, ContainerExecutor.class);

    // TestMRJobs is for testing non-uberized operation only; see TestUberAM
    // for corresponding uberized tests.
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();

    //need to do this because historyServer.init creates a new Configuration
    getConfig().set(JHAdminConfig.MR_HISTORY_ADDRESS,
                    historyServer.getConfig().get(JHAdminConfig.MR_HISTORY_ADDRESS));
    MRWebAppUtil.setJHSWebappURLWithoutScheme(getConfig(),
        MRWebAppUtil.getJHSWebappURLWithoutScheme(historyServer.getConfig()));

    LOG.info("MiniMRYARN ResourceManager address: " +
        getConfig().get(YarnConfiguration.RM_ADDRESS));
    LOG.info("MiniMRYARN ResourceManager web address: " +
        WebAppUtils.getRMWebAppURLWithoutScheme(getConfig()));
    LOG.info("MiniMRYARN HistoryServer address: " +
        getConfig().get(JHAdminConfig.MR_HISTORY_ADDRESS));
    LOG.info("MiniMRYARN HistoryServer web address: " +
        getResolvedMRHistoryWebAppURLWithoutScheme(getConfig(),
            MRWebAppUtil.getJHSHttpPolicy() == HttpConfig.Policy.HTTPS_ONLY));
  }

  private class JobHistoryServerWrapper extends AbstractService {
    public JobHistoryServerWrapper() {
      super(JobHistoryServerWrapper.class.getName());
    }
    private volatile boolean jhsStarted = false;

    @Override
    public synchronized void serviceStart() throws Exception {
      try {
        if (!getConfig().getBoolean(
            JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS,
            JHAdminConfig.DEFAULT_MR_HISTORY_MINICLUSTER_FIXED_PORTS)) {
          String hostname = MiniYARNCluster.getHostname();
          // pick free random ports.
          getConfig().set(JHAdminConfig.MR_HISTORY_ADDRESS,
            hostname + ":0");
          MRWebAppUtil.setJHSWebappURLWithoutScheme(getConfig(), hostname
              + ":0");
          getConfig().set(JHAdminConfig.JHS_ADMIN_ADDRESS,
            hostname + ":0");
        }
        historyServer = new JobHistoryServer();
        historyServer.init(getConfig());
        new Thread() {
          public void run() {
            historyServer.start();
            jhsStarted = true;
          };
        }.start();

        while (!jhsStarted) {
          LOG.info("Waiting for HistoryServer to start...");
          Thread.sleep(1500);
        }
        //TODO Add a timeout. State.STOPPED check ?
        if (historyServer.getServiceState() != STATE.STARTED) {
          throw new IOException("HistoryServer failed to start");
        }
        super.serviceStart();
      } catch (Throwable t) {
        throw new YarnRuntimeException(t);
      }
    }

    @Override
    public synchronized void serviceStop() throws Exception {
      if (historyServer != null) {
        historyServer.stop();
      }
      super.serviceStop();
    }
  }

  public JobHistoryServer getHistoryServer() {
    return this.historyServer;
  }
}
