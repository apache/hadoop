/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.common.StorageInfo;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

/**
 * This class provides a command line interface to start the SCM
 * using Picocli.
 */

@Command(name = "ozone scm",
    hidden = true, description = "Start or initialize the scm server.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class StorageContainerManagerStarter extends GenericCli {

  private OzoneConfiguration conf;
  private SCMStarterInterface receiver;
  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerManagerStarter.class);

  public static void main(String[] args) throws Exception {
    TracingUtil.initTracing("StorageContainerManager");
    new StorageContainerManagerStarter(
        new StorageContainerManagerStarter.SCMStarterHelper()).run(args);
  }

  public StorageContainerManagerStarter(SCMStarterInterface receiverObj) {
    super();
    receiver = receiverObj;
  }

  @Override
  public Void call() throws Exception {
    commonInit();
    startScm();
    return null;
  }

  /**
   * This function implements a sub-command to generate a new
   * cluster ID from the command line.
   */
  @CommandLine.Command(name = "--genclusterid",
      customSynopsis = "ozone scm [global options] --genclusterid [options]",
      hidden = false,
      description = "Generate a new Cluster ID",
      mixinStandardHelpOptions = true,
      versionProvider = HddsVersionProvider.class)
  public void generateClusterId() {
    commonInit();
    System.out.println("Generating new cluster id:");
    System.out.println(receiver.generateClusterId());
  }

  /**
   * This function implements a sub-command to allow the SCM to be
   * initialized from the command line.
   *
   * @param clusterId - Cluster ID to use when initializing. If null,
   *                  a random ID will be generated and used.
   */
  @CommandLine.Command(name = "--init",
      customSynopsis = "ozone scm [global options] --init [options]",
      hidden = false,
      description = "Initialize the SCM if not already initialized",
      mixinStandardHelpOptions = true,
      versionProvider = HddsVersionProvider.class)
  public void initScm(@CommandLine.Option(names = { "--clusterid" },
      description = "Optional: The cluster id to use when formatting SCM",
      paramLabel = "id") String clusterId)
      throws Exception {
    commonInit();
    boolean result = receiver.init(conf, clusterId);
    if (!result) {
      throw new IOException("scm init failed");
    }
  }

  /**
   * This function is used by the command line to start the SCM.
   */
  private void startScm() throws Exception {
    receiver.start(conf);
  }

  /**
   * This function should be called by each command to ensure the configuration
   * is set and print the startup banner message.
   */
  private void commonInit() {
    conf = createOzoneConfiguration();

    String[] originalArgs = getCmd().getParseResult().originalArgs()
        .toArray(new String[0]);
    StringUtils.startupShutdownMessage(StorageContainerManager.class,
        originalArgs, LOG);
  }

  /**
   * This static class wraps the external dependencies needed for this command
   * to execute its tasks. This allows the dependency to be injected for unit
   * testing.
   */
  static class SCMStarterHelper implements SCMStarterInterface {

    public void start(OzoneConfiguration conf) throws Exception {
      StorageContainerManager stm = StorageContainerManager.createSCM(conf);
      stm.start();
      stm.join();
    }

    public boolean init(OzoneConfiguration conf, String clusterId)
        throws IOException{
      return StorageContainerManager.scmInit(conf, clusterId);
    }

    public String generateClusterId() {
      return StorageInfo.newClusterID();
    }
  }
}