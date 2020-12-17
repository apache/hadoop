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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.ha.FailoverController;
import org.apache.hadoop.ha.FailoverFailedException;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocolHelper;
import org.apache.hadoop.ha.ServiceFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class to extend HAAdmin to do a little bit of HDFS-specific configuration.
 */
public class DFSHAAdmin extends HAAdmin {

  private static final String FORCEFENCE  = "forcefence";
  private static final Logger LOG = LoggerFactory.getLogger(DFSHAAdmin.class);

  private String nameserviceId;
  private final static Map<String, UsageInfo> USAGE_DFS_ONLY =
      ImmutableMap.<String, UsageInfo> builder()
          .put("-transitionToObserver", new UsageInfo("<serviceId>",
                  "Transitions the service into Observer state"))
          .put("-failover", new UsageInfo(
              "[--"+FORCEFENCE+"] [--"+FORCEACTIVE+"] "
                  + "<serviceId> <serviceId>",
              "Failover from the first service to the second.\n"
                  + "Unconditionally fence services if the --" + FORCEFENCE
                  + " option is used.\n"
                  + "Try to failover to the target service "
                  + "even if it is not ready if the "
                  + "--" + FORCEACTIVE + " option is used.")).build();

  private final static Map<String, UsageInfo> USAGE_DFS_MERGED =
      ImmutableSortedMap.<String, UsageInfo> naturalOrder()
          .putAll(USAGE)
          .putAll(USAGE_DFS_ONLY)
          .build();

  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }
  
  protected void setOut(PrintStream out) {
    this.out = out;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  /**
   * Add the requisite security principal settings to the given Configuration,
   * returning a copy.
   * @param conf the original config
   * @return a copy with the security settings added
   */
  public static Configuration addSecurityConfiguration(Configuration conf) {
    // Make a copy so we don't mutate it. Also use an HdfsConfiguration to
    // force loading of hdfs-site.xml.
    conf = new HdfsConfiguration(conf);
    String nameNodePrincipal = conf.get(
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using NN principal: " + nameNodePrincipal);
    }

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        nameNodePrincipal);
    return conf;
  }

  /**
   * Try to map the given namenode ID to its service address.
   */
  @Override
  protected HAServiceTarget resolveTarget(String nnId) {
    HdfsConfiguration conf = (HdfsConfiguration)getConf();
    return new NNHAServiceTarget(conf, nameserviceId, nnId);
  }

  @Override
  protected String getUsageString() {
    return "Usage: haadmin [-ns <nameserviceId>]";
  }

  /**
   * Add CLI options which are specific to the failover command and no
   * others.
   */
  private void addFailoverCliOpts(Options failoverOpts) {
    failoverOpts.addOption(FORCEFENCE, false, "force fencing");
    failoverOpts.addOption(FORCEACTIVE, false, "force failover");
    // Don't add FORCEMANUAL, since that's added separately for all commands
    // that change state.
  }
  @Override
  protected boolean checkParameterValidity(String[] argv){
    return  checkParameterValidity(argv, USAGE_DFS_MERGED);
  }

  @Override
  protected int runCmd(String[] argv) throws Exception {

    if(argv.length < 1){
      printUsage(errOut, USAGE_DFS_MERGED);
      return -1;
    }

    int i = 0;
    String cmd = argv[i++];
    //Process "-ns" Option
    if ("-ns".equals(cmd)) {
      if (i == argv.length) {
        errOut.println("Missing nameservice ID");
        printUsage(errOut, USAGE_DFS_MERGED);
        return -1;
      }
      nameserviceId = argv[i++];
      if (i >= argv.length) {
        errOut.println("Missing command");
        printUsage(errOut, USAGE_DFS_MERGED);
        return -1;
      }
      argv = Arrays.copyOfRange(argv, i, argv.length);
      cmd = argv[0];
    }

    if (!checkParameterValidity(argv)){
      return -1;
    }

    /*
       "-help" command has to to be handled here because it should
       be supported both by HAAdmin and DFSHAAdmin but it is contained in
       USAGE_DFS_ONLY
    */
    if ("-help".equals(cmd)){
      return help(argv, USAGE_DFS_MERGED);
    }

    if (!USAGE_DFS_ONLY.containsKey(cmd)) {
      return super.runCmd(argv);
    }

    Options opts = new Options();
    // Add command-specific options
    if ("-failover".equals(cmd)) {
      addFailoverCliOpts(opts);
    }
    // Mutative commands take FORCEMANUAL option
    if ("-transitionToObserver".equals(cmd) ||
        "-failover".equals(cmd)) {
      opts.addOption(FORCEMANUAL, false,
          "force manual control even if auto-failover is enabled");
    }
    CommandLine cmdLine = parseOpts(cmd, opts, argv, USAGE_DFS_MERGED);
    if (cmdLine == null) {
      return -1;
    }

    if (cmdLine.hasOption(FORCEMANUAL)) {
      if (!confirmForceManual()) {
        LOG.error("Aborted");
        return -1;
      }
      // Instruct the NNs to honor this request even if they're
      // configured for manual failover.
      setRequestSource(RequestSource.REQUEST_BY_USER_FORCED);
    }

    if ("-transitionToObserver".equals(cmd)) {
      return transitionToObserver(cmdLine);
    } else if ("-failover".equals(cmd)) {
      return failover(cmdLine);
    } else {
      // This line should not be reached
      throw new AssertionError("Should not get here, command: " + cmd);
    }
  }
  
  /**
   * returns the list of all namenode ids for the given configuration.
   */
  @Override
  protected Collection<String> getTargetIds(String namenodeToActivate) {
    return DFSUtilClient.getNameNodeIds(
        getConf(), (nameserviceId != null)?
            nameserviceId : DFSUtil.getNamenodeNameServiceId(getConf()));
  }

  /**
   * Check if the target supports the Observer state.
   * @param target the target to check
   * @return true if the target support Observer state, false otherwise.
   */
  private boolean checkSupportObserver(HAServiceTarget target) {
    if (target.supportObserver()) {
      return true;
    } else {
      errOut.println(
          "The target " + target + " doesn't support Observer state.");
      return false;
    }
  }

  private int transitionToObserver(final CommandLine cmd)
      throws IOException, ServiceFailedException {
    String[] argv = cmd.getArgs();
    if (argv.length != 1) {
      errOut.println("transitionToObserver: incorrect number of arguments");
      printUsage(errOut, "-transitionToObserver", USAGE_DFS_MERGED);
      return -1;
    }

    HAServiceTarget target = resolveTarget(argv[0]);
    if (!checkSupportObserver(target)) {
      return -1;
    }
    if (!checkManualStateManagementOK(target)) {
      return -1;
    }
    HAServiceProtocol proto = target.getProxy(getConf(), 0);
    HAServiceProtocolHelper.transitionToObserver(proto, createReqInfo());
    return 0;
  }

  private int failover(CommandLine cmd)
      throws IOException, ServiceFailedException {
    boolean forceFence = cmd.hasOption(FORCEFENCE);
    boolean forceActive = cmd.hasOption(FORCEACTIVE);

    int numOpts = cmd.getOptions() == null ? 0 : cmd.getOptions().length;
    final String[] args = cmd.getArgs();

    if (numOpts > 3 || args.length != 2) {
      errOut.println("failover: incorrect arguments");
      printUsage(errOut, "-failover", USAGE_DFS_MERGED);
      return -1;
    }

    HAServiceTarget fromNode = resolveTarget(args[0]);
    HAServiceTarget toNode = resolveTarget(args[1]);

    fromNode.setTransitionTargetHAStatus(
        HAServiceProtocol.HAServiceState.STANDBY);
    toNode.setTransitionTargetHAStatus(
        HAServiceProtocol.HAServiceState.ACTIVE);

    // Check that auto-failover is consistently configured for both nodes.
    Preconditions.checkState(
        fromNode.isAutoFailoverEnabled() ==
            toNode.isAutoFailoverEnabled(),
        "Inconsistent auto-failover configs between %s and %s!",
        fromNode, toNode);

    if (fromNode.isAutoFailoverEnabled()) {
      if (forceFence || forceActive) {
        // -forceActive doesn't make sense with auto-HA, since, if the node
        // is not healthy, then its ZKFC will immediately quit the election
        // again the next time a health check runs.
        //
        // -forceFence doesn't seem to have any real use cases with auto-HA
        // so it isn't implemented.
        errOut.println(FORCEFENCE + " and " + FORCEACTIVE + " flags not " +
            "supported with auto-failover enabled.");
        return -1;
      }
      try {
        return gracefulFailoverThroughZKFCs(toNode);
      } catch (UnsupportedOperationException e){
        errOut.println("Failover command is not supported with " +
            "auto-failover enabled: " + e.getLocalizedMessage());
        return -1;
      }
    }

    FailoverController fc =
        new FailoverController(getConf(), getRequestSource());

    try {
      fc.failover(fromNode, toNode, forceFence, forceActive);
      out.println("Failover from "+args[0]+" to "+args[1]+" successful");
    } catch (FailoverFailedException ffe) {
      errOut.println("Failover failed: " + ffe.getLocalizedMessage());
      return -1;
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new DFSHAAdmin(), argv);
    System.exit(res);
  }
}
