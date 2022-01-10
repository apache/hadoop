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
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedure;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceJob;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedureScheduler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.FORCE_CLOSE_OPEN;
import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.MAP;
import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.BANDWIDTH;
import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.TRASH;
import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.DELAY_DURATION;
import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.CLI_OPTIONS;
import static org.apache.hadoop.tools.fedbalance.FedBalanceOptions.DIFF_THRESHOLD;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;

/**
 * Balance data from src cluster to dst cluster with distcp.
 *
 * 1. Move data from the source path to the destination path with distcp.
 * 2. Delete the source path to trash.
 */
public class FedBalance extends Configured implements Tool {

  public static final Logger LOG =
      LoggerFactory.getLogger(FedBalance.class);
  private static final String SUBMIT_COMMAND = "submit";
  private static final String CONTINUE_COMMAND = "continue";
  public static final String NO_MOUNT = "no-mount";
  public static final String DISTCP_PROCEDURE = "distcp-procedure";
  public static final String TRASH_PROCEDURE = "trash-procedure";

  public static final String FED_BALANCE_DEFAULT_XML =
      "hdfs-fedbalance-default.xml";
  public static final String FED_BALANCE_SITE_XML = "hdfs-fedbalance-site.xml";

  /**
   * This class helps building the balance job.
   */
  private final class Builder {
    /* Force close all open files while there is no diff. */
    private boolean forceCloseOpen = false;
    /* Max number of concurrent maps to use for copy. */
    private int map = 10;
    /* Specify bandwidth per map in MB. */
    private int bandwidth = 10;
    /* Specify the trash behaviour of the source path. */
    private TrashOption trashOpt = TrashOption.TRASH;
    /* Specify the duration(millie seconds) when the procedure needs retry. */
    private long delayDuration = TimeUnit.SECONDS.toMillis(1);
    /* Specify the threshold of diff entries. */
    private int diffThreshold = 0;
    /* The source input. This specifies the source path. */
    private final String inputSrc;
    /* The dst input. This specifies the dst path. */
    private final String inputDst;

    private Builder(String inputSrc, String inputDst) {
      this.inputSrc = inputSrc;
      this.inputDst = inputDst;
    }

    /**
     * Whether force close all open files while there is no diff.
     * @param value true if force close all the open files.
     */
    public Builder setForceCloseOpen(boolean value) {
      this.forceCloseOpen = value;
      return this;
    }

    /**
     * Max number of concurrent maps to use for copy.
     * @param value the map number of the distcp.
     */
    public Builder setMap(int value) {
      this.map = value;
      return this;
    }

    /**
     * Specify bandwidth per map in MB.
     * @param value the bandwidth.
     */
    public Builder setBandWidth(int value) {
      this.bandwidth = value;
      return this;
    }

    /**
     * Specify the trash behaviour of the source path.
     * @param value the trash option.
     */
    public Builder setTrashOpt(TrashOption value) {
      this.trashOpt = value;
      return this;
    }

    /**
     * Specify the duration(millie seconds) when the procedure needs retry.
     * @param value the delay duration of the job.
     */
    public Builder setDelayDuration(long value) {
      this.delayDuration = value;
      return this;
    }

    /**
     * Specify the threshold of diff entries.
     * @param value the threshold of a fast distcp.
     */
    public Builder setDiffThreshold(int value) {
      this.diffThreshold = value;
      return this;
    }

    /**
     * Build the balance job.
     */
    public BalanceJob build() throws IOException {
      // Construct job context.
      FedBalanceContext context;
      Path dst = new Path(inputDst);
      if (dst.toUri().getAuthority() == null) {
        throw new IOException("The destination cluster must be specified.");
      }
      Path src = new Path(inputSrc);
      if (src.toUri().getAuthority() == null) {
        throw new IOException("The source cluster must be specified.");
      }
      context = new FedBalanceContext.Builder(src, dst, NO_MOUNT, getConf())
          .setForceCloseOpenFiles(forceCloseOpen).setUseMountReadOnly(false)
          .setMapNum(map).setBandwidthLimit(bandwidth).setTrash(trashOpt)
          .setDiffThreshold(diffThreshold).build();

      LOG.info(context.toString());
      // Construct the balance job.
      BalanceJob.Builder<BalanceProcedure> builder = new BalanceJob.Builder<>();
      DistCpProcedure dcp =
          new DistCpProcedure(DISTCP_PROCEDURE, null, delayDuration, context);
      builder.nextProcedure(dcp);
      TrashProcedure tp =
          new TrashProcedure(TRASH_PROCEDURE, null, delayDuration, context);
      builder.nextProcedure(tp);
      return builder.build();
    }
  }

  public FedBalance() {
    super();
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLineParser parser = new GnuParser();
    CommandLine command =
        parser.parse(FedBalanceOptions.CLI_OPTIONS, args, true);
    String[] leftOverArgs = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      printUsage();
      return -1;
    }
    String cmd = leftOverArgs[0];
    if (cmd.equals(SUBMIT_COMMAND)) {
      if (leftOverArgs.length < 3) {
        printUsage();
        return -1;
      }
      String inputSrc = leftOverArgs[1];
      String inputDst = leftOverArgs[2];
      return submit(command, inputSrc, inputDst);
    } else if (cmd.equals(CONTINUE_COMMAND)) {
      return continueJob();
    } else {
      printUsage();
      return -1;
    }
  }

  /**
   * Recover and continue the unfinished jobs.
   */
  private int continueJob() throws InterruptedException {
    BalanceProcedureScheduler scheduler =
        new BalanceProcedureScheduler(getConf());
    try {
      scheduler.init(true);
      while (true) {
        Collection<BalanceJob> jobs = scheduler.getAllJobs();
        int unfinished = 0;
        for (BalanceJob job : jobs) {
          if (!job.isJobDone()) {
            unfinished++;
          }
          LOG.info(job.toString());
        }
        if (unfinished == 0) {
          break;
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      }
    } catch (IOException e) {
      LOG.error("Continue balance job failed.", e);
      return -1;
    } finally {
      scheduler.shutDown();
    }
    return 0;
  }

  /**
   * Start a ProcedureScheduler and submit the job.
   *
   * @param command the command options.
   * @param inputSrc the source input. This specifies the source path.
   * @param inputDst the dst input. This specifies the dst path.
   */
  private int submit(CommandLine command, String inputSrc, String inputDst)
      throws IOException {
    Builder builder = new Builder(inputSrc, inputDst);
    // parse options.
    builder.setForceCloseOpen(command.hasOption(FORCE_CLOSE_OPEN.getOpt()));
    if (command.hasOption(MAP.getOpt())) {
      builder.setMap(Integer.parseInt(command.getOptionValue(MAP.getOpt())));
    }
    if (command.hasOption(BANDWIDTH.getOpt())) {
      builder.setBandWidth(
          Integer.parseInt(command.getOptionValue(BANDWIDTH.getOpt())));
    }
    if (command.hasOption(DELAY_DURATION.getOpt())) {
      builder.setDelayDuration(
          Long.parseLong(command.getOptionValue(DELAY_DURATION.getOpt())));
    }
    if (command.hasOption(DIFF_THRESHOLD.getOpt())) {
      builder.setDiffThreshold(Integer.parseInt(
          command.getOptionValue(DIFF_THRESHOLD.getOpt())));
    }
    if (command.hasOption(TRASH.getOpt())) {
      String val = command.getOptionValue(TRASH.getOpt());
      if (val.equalsIgnoreCase("skip")) {
        builder.setTrashOpt(TrashOption.SKIP);
      } else if (val.equalsIgnoreCase("trash")) {
        builder.setTrashOpt(TrashOption.TRASH);
      } else if (val.equalsIgnoreCase("delete")) {
        builder.setTrashOpt(TrashOption.DELETE);
      } else {
        printUsage();
        return -1;
      }
    }

    // Submit the job.
    BalanceProcedureScheduler scheduler =
        new BalanceProcedureScheduler(getConf());
    scheduler.init(false);
    try {
      BalanceJob balanceJob = builder.build();
      // Submit and wait until the job is done.
      scheduler.submit(balanceJob);
      scheduler.waitUntilDone(balanceJob);
    } catch (IOException e) {
      LOG.error("Submit balance job failed.", e);
      return -1;
    } finally {
      scheduler.shutDown();
    }
    return 0;
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
        "fedbalance OPTIONS [submit|continue] <src> <target>\n\nOPTIONS",
        CLI_OPTIONS);
  }

  /**
   * Loads properties from hdfs-fedbalance-default.xml into configuration
   * object.
   *
   * @return Configuration which includes properties from
   *         hdfs-fedbalance-default.xml and hdfs-fedbalance-site.xml
   */
  @VisibleForTesting
  static Configuration getDefaultConf() {
    Configuration config = new Configuration();
    config.addResource(FED_BALANCE_DEFAULT_XML);
    config.addResource(FED_BALANCE_SITE_XML);
    return config;
  }

  /**
   * Main function of the FedBalance program. Parses the input arguments and
   * invokes the FedBalance::run() method, via the ToolRunner.
   * @param argv Command-line arguments sent to FedBalance.
   */
  public static void main(String[] argv) {
    Configuration conf = getDefaultConf();
    FedBalance fedBalance = new FedBalance();
    fedBalance.setConf(conf);
    int exitCode;
    try {
      exitCode = ToolRunner.run(fedBalance, argv);
    } catch (Exception e) {
      LOG.warn("Couldn't complete FedBalance operation.", e);
      exitCode = -1;
    }
    System.exit(exitCode);
  }
}
