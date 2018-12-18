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
package org.apache.hadoop.ozone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Datanode service plugin to start the HDDS container services.
 */

@Command(name = "ozone datanode",
    hidden = true, description = "Start the datanode for ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class HddsDatanodeService extends GenericCli implements ServicePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsDatanodeService.class);

  private OzoneConfiguration conf;
  private DatanodeDetails datanodeDetails;
  private DatanodeStateMachine datanodeStateMachine;
  private List<ServicePlugin> plugins;

  /**
   * Default constructor.
   */
  public HddsDatanodeService() {
    this(null);
  }

  /**
   * Constructs {@link HddsDatanodeService} using the provided {@code conf}
   * value.
   *
   * @param conf OzoneConfiguration
   */
  public HddsDatanodeService(Configuration conf) {
    if (conf == null) {
      this.conf = new OzoneConfiguration();
    } else {
      this.conf = new OzoneConfiguration(conf);
    }
  }

  @VisibleForTesting
  public static HddsDatanodeService createHddsDatanodeService(
      String[] args, Configuration conf) {
    return createHddsDatanodeService(args, conf, true);
  }

  /**
   * Create an Datanode instance based on the supplied command-line arguments.
   * <p>
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal handlers.
   *
   * @param args        command line arguments.
   * @param conf        HDDS configuration
   * @param printBanner if true, then log a verbose startup message.
   * @return Datanode instance
   */
  private static HddsDatanodeService createHddsDatanodeService(
      String[] args, Configuration conf, boolean printBanner) {
    if (args.length == 0 && printBanner) {
      StringUtils
          .startupShutdownMessage(HddsDatanodeService.class, args, LOG);
      return new HddsDatanodeService(conf);
    } else {
      new HddsDatanodeService().run(args);
      return null;
    }
  }

  public static void main(String[] args) {
    try {
      Configuration conf = new OzoneConfiguration();
      HddsDatanodeService hddsDatanodeService =
          createHddsDatanodeService(args, conf, true);
      if (hddsDatanodeService != null) {
        hddsDatanodeService.start(null);
        hddsDatanodeService.join();
      }
    } catch (Throwable e) {
      LOG.error("Exception in HddsDatanodeService.", e);
      terminate(1, e);
    }
  }

  /**
   * Starts HddsDatanode services.
   *
   * @param service The service instance invoking this method
   */
  @Override
  public void start(Object service) {
    DefaultMetricsSystem.initialize("HddsDatanode");
    OzoneConfiguration.activate();
    if (service instanceof Configurable) {
      conf = new OzoneConfiguration(((Configurable) service).getConf());
    }
    if (HddsUtils.isHddsEnabled(conf)) {
      try {
        String hostname = HddsUtils.getHostName(conf);
        String ip = InetAddress.getByName(hostname).getHostAddress();
        datanodeDetails = initializeDatanodeDetails();
        datanodeDetails.setHostName(hostname);
        datanodeDetails.setIpAddress(ip);
        datanodeStateMachine = new DatanodeStateMachine(datanodeDetails, conf);
        startPlugins();
        // Starting HDDS Daemons
        datanodeStateMachine.startDaemon();
      } catch (IOException e) {
        throw new RuntimeException("Can't start the HDDS datanode plugin", e);
      }
    }
  }

  /**
   * Returns DatanodeDetails or null in case of Error.
   *
   * @return DatanodeDetails
   */
  private DatanodeDetails initializeDatanodeDetails()
      throws IOException {
    String idFilePath = HddsUtils.getDatanodeIdFilePath(conf);
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid file path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID);
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_DATANODE_ID +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration" +
          " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    if (idFile.exists()) {
      return ContainerUtils.readDatanodeDetailsFrom(idFile);
    } else {
      // There is no datanode.id file, this might be the first time datanode
      // is started.
      String datanodeUuid = UUID.randomUUID().toString();
      return DatanodeDetails.newBuilder().setUuid(datanodeUuid).build();
    }
  }

  /**
   * Starts all the service plugins which are configured using
   * OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY.
   */
  private void startPlugins() {
    try {
      plugins = conf.getInstances(HDDS_DATANODE_PLUGINS_KEY,
          ServicePlugin.class);
    } catch (RuntimeException e) {
      String pluginsValue = conf.get(HDDS_DATANODE_PLUGINS_KEY);
      LOG.error("Unable to load HDDS DataNode plugins. " +
              "Specified list of plugins: {}",
          pluginsValue, e);
      throw e;
    }
    for (ServicePlugin plugin : plugins) {
      try {
        plugin.start(this);
        LOG.info("Started plug-in {}", plugin);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin {} could not be started", plugin, t);
      }
    }
  }

  /**
   * Returns the OzoneConfiguration used by this HddsDatanodeService.
   *
   * @return OzoneConfiguration
   */
  public OzoneConfiguration getConf() {
    return conf;
  }

  /**
   * Return DatanodeDetails if set, return null otherwise.
   *
   * @return DatanodeDetails
   */
  @VisibleForTesting
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  @VisibleForTesting
  public DatanodeStateMachine getDatanodeStateMachine() {
    return datanodeStateMachine;
  }

  public void join() {
    try {
      datanodeStateMachine.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  @Override
  public void stop() {
    if (plugins != null) {
      for (ServicePlugin plugin : plugins) {
        try {
          plugin.stop();
          LOG.info("Stopped plug-in {}", plugin);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin {} could not be stopped", plugin, t);
        }
      }
    }
    if (datanodeStateMachine != null) {
      datanodeStateMachine.stopDaemon();
    }
  }

  @Override
  public void close() {
    if (plugins != null) {
      for (ServicePlugin plugin : plugins) {
        try {
          plugin.close();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin {} could not be closed", plugin, t);
        }
      }
    }
  }
}
