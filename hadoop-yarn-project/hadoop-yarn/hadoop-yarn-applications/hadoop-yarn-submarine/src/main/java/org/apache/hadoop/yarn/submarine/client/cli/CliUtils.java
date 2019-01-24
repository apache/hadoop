/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineRuntimeException;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.submarine.client.cli.CliConstants.KEYTAB;
import static org.apache.hadoop.yarn.submarine.client.cli.CliConstants.PRINCIPAL;

public class CliUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(CliUtils.class);
  /**
   * Replace patterns inside cli
   *
   * @return launch command after pattern replace
   */
  public static String replacePatternsInLaunchCommand(String specifiedCli,
      RunJobParameters jobRunParameters,
      RemoteDirectoryManager directoryManager) throws IOException {
    String input = jobRunParameters.getInputPath();
    String jobDir = jobRunParameters.getCheckpointPath();
    String savedModelDir = jobRunParameters.getSavedModelPath();

    Map<String, String> replacePattern = new HashMap<>();
    if (jobDir != null) {
      replacePattern.put("%" + CliConstants.CHECKPOINT_PATH + "%", jobDir);
    }
    if (input != null) {
      replacePattern.put("%" + CliConstants.INPUT_PATH + "%", input);
    }
    if (savedModelDir != null) {
      replacePattern.put("%" + CliConstants.SAVED_MODEL_PATH + "%",
          savedModelDir);
    }

    String newCli = specifiedCli;
    for (Map.Entry<String, String> replace : replacePattern.entrySet()) {
      newCli = newCli.replace(replace.getKey(), replace.getValue());
    }

    return newCli;
  }

  // Is it for help?
  public static boolean argsForHelp(String[] args) {
    if (args == null || args.length == 0)
      return true;

    if (args.length == 1) {
      return args[0].equals("-h") || args[0].equals("--help");
    }

    return false;
  }

  public static void doLoginIfSecure(String keytab, String principal) throws
      IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    if (StringUtils.isEmpty(keytab) || StringUtils.isEmpty(principal)) {
      if (StringUtils.isNotEmpty(keytab)) {
        SubmarineRuntimeException e = new SubmarineRuntimeException("The " +
            "parameter of " + PRINCIPAL + " is missing.");
        LOG.error(e.getMessage(), e);
        throw e;
      }

      if (StringUtils.isNotEmpty(principal)) {
        SubmarineRuntimeException e = new SubmarineRuntimeException("The " +
            "parameter of " + KEYTAB + " is missing.");
        LOG.error(e.getMessage(), e);
        throw e;
      }

      UserGroupInformation user = UserGroupInformation.getCurrentUser();
      if(user == null || user.getAuthenticationMethod() ==
          UserGroupInformation.AuthenticationMethod.SIMPLE) {
        SubmarineRuntimeException e = new SubmarineRuntimeException("Failed " +
            "to authenticate in secure environment. Please run kinit " +
            "command in advance or use " + "--" + KEYTAB + "/--" + PRINCIPAL +
            " parameters");
        LOG.error(e.getMessage(), e);
        throw e;
      }
      LOG.info("Submarine job is submitted by user: " + user.getUserName());
      return;
    }

    File keytabFile = new File(keytab);
    if (!keytabFile.exists()) {
      SubmarineRuntimeException e =  new SubmarineRuntimeException("No " +
          "keytab localized at  " + keytab);
      LOG.error(e.getMessage(), e);
      throw e;
    }
    UserGroupInformation.loginUserFromKeytab(principal, keytab);
  }
}
