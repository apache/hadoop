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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Cli {
  private static final Logger LOG =
      LoggerFactory.getLogger(Cli.class);

  private static void printHelp() {
    StringBuilder helpMsg = new StringBuilder();
    helpMsg.append("\n\nUsage: <object> [<action>] [<args>]\n");
    helpMsg.append("  Below are all objects / actions:\n");
    helpMsg.append("    job \n");
    helpMsg.append("       run : run a job, please see 'job run --help' for usage \n");
    helpMsg.append("       show : get status of job, please see 'job show --help' for usage \n");

    System.out.println(helpMsg.toString());
  }

  private static ClientContext getClientContext() {
    Configuration conf = new YarnConfiguration();
    ClientContext clientContext = new ClientContext();
    clientContext.setConfiguration(conf);
    RuntimeFactory runtimeFactory = RuntimeFactory.getRuntimeFactory(
        clientContext);
    clientContext.setRuntimeFactory(runtimeFactory);
    return clientContext;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("              _                              _              \n"
        + "             | |                            (_)             \n"
        + "  ___  _   _ | |__   _ __ ___    __ _  _ __  _  _ __    ___ \n"
        + " / __|| | | || '_ \\ | '_ ` _ \\  / _` || '__|| || '_ \\  / _ \\\n"
        + " \\__ \\| |_| || |_) || | | | | || (_| || |   | || | | ||  __/\n"
        + " |___/ \\__,_||_.__/ |_| |_| |_| \\__,_||_|   |_||_| |_| \\___|\n"
        + "                                                    \n"
        + "                             ?\n"
        + " ~~~~~~~~~~~~~~~~~~~~~~~~~~~|^\"~~~~~~~~~~~~~~~~~~~~~~~~~o~~~~~~~~~~~\n"
        + "        o                   |                  o      __o\n"
        + "         o                  |                 o     |X__>\n"
        + "       ___o                 |                __o\n"
        + "     (X___>--             __|__            |X__>     o\n"
        + "                         |     \\                   __o\n"
        + "                         |      \\                |X__>\n"
        + "  _______________________|_______\\________________\n"
        + " <                                                \\____________   _\n"
        + "  \\                                                            \\ (_)\n"
        + "   \\    O       O       O                                       >=)\n"
        + "    \\__________________________________________________________/ (_)\n"
        + "\n");

    if (CliUtils.argsForHelp(args)) {
      printHelp();
      System.exit(0);
    }

    if (args.length < 2) {
      LOG.error("Bad parameters specified.");
      printHelp();
      System.exit(-1);
    }

    String[] moduleArgs = Arrays.copyOfRange(args, 2, args.length);
    ClientContext clientContext = getClientContext();

    if (args[0].equals("job")) {
      String subCmd = args[1];
      if (subCmd.equals(CliConstants.RUN)) {
        new RunJobCli(clientContext).run(moduleArgs);
      } else if (subCmd.equals(CliConstants.SHOW)) {
        new ShowJobCli(clientContext).run(moduleArgs);
      } else {
        printHelp();
        throw new IllegalArgumentException("Unknown option for job");
      }
    } else {
      printHelp();
      throw new IllegalArgumentException("Bad parameters <TODO>");
    }
  }
}
