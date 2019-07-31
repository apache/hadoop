/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

import static org.apache.hadoop.yarn.client.api.AppAdminClient.DEFAULT_TYPE;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.KillJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class KillJobCli extends AbstractCli {
  private static final Logger LOG = LoggerFactory.getLogger(ShowJobCli.class);

  private Options options;
  private ParametersHolder parametersHolder;

  public KillJobCli(ClientContext cliContext) {
    super(cliContext);
    options = generateOptions();
  }

  public void printUsages() {
    new HelpFormatter().printHelp("job kill", options);
  }

  private Options generateOptions() {
    Options options = new Options();
    options.addOption(CliConstants.NAME, true, "Name of the job");
    options.addOption("h", "help", false, "Print help");
    return options;
  }

  private void parseCommandLineAndGetKillJobParameters(String[] args)
      throws IOException, YarnException {
    // Do parsing
    GnuParser parser = new GnuParser();
    CommandLine cli;
    try {
      cli = parser.parse(options, args);
      parametersHolder =
          ParametersHolder.createWithCmdLine(cli, Command.KILL_JOB);
      parametersHolder.updateParameters(clientContext);
    } catch (ParseException e) {
      LOG.error(("Error parsing command-line options: " + e.getMessage()));
      printUsages();
    }
  }

  @VisibleForTesting
  protected boolean killJob() throws IOException, YarnException {
    String jobName = getParameters().getName();
    AppAdminClient appAdminClient = AppAdminClient
        .createAppAdminClient(DEFAULT_TYPE, clientContext.getYarnConfig());

    if (appAdminClient.actionStop(jobName) != 0) {
      LOG.error("appAdminClient fail to stop application");
      return false;
    }
    if (appAdminClient.actionDestroy(jobName) != 0) {
      LOG.error("appAdminClient fail to destroy application");
      return false;
    }

    appAdminClient.stop();
    return true;
  }

  @VisibleForTesting
  public KillJobParameters getParameters() {
    return (KillJobParameters) parametersHolder.getParameters();
  }

  @Override
  public int run(String[] args) throws ParseException, IOException,
      YarnException, InterruptedException, SubmarineException {
    if (CliUtils.argsForHelp(args)) {
      printUsages();
      return 0;
    }
    parseCommandLineAndGetKillJobParameters(args);
    if (killJob() == true) {
      LOG.info("Kill job successfully !");
    }
    return 0;
  }

}
