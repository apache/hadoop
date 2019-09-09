/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param;

import static org.apache.hadoop.yarn.submarine.client.cli.runjob.RunJobCli.YAML_PARSE_FAILED;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.Command;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Configs;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Role;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Roles;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Scheduling;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Security;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.TensorBoard;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlConfigFile;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlParseException;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.Framework;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class acts as a wrapper of {@code CommandLine} values along with
 * YAML configuration values.
 * YAML configuration is only stored if the -f &lt;filename&gt;
 * option is specified along the CLI arguments.
 * Using this wrapper class makes easy to deal with
 * any form of configuration source potentially added into Submarine,
 * in the future.
 * If both YAML and CLI value is found for a config, this is an error case.
 */
public final class ParametersHolder {
  private static final Logger LOG =
      LoggerFactory.getLogger(ParametersHolder.class);

  public static final String SUPPORTED_FRAMEWORKS_MESSAGE =
      "TensorFlow and PyTorch are the only supported frameworks for now!";
  public static final String SUPPORTED_COMMANDS_MESSAGE =
      "'Show job' and 'run job' are the only supported commands for now!";



  private final CommandLine parsedCommandLine;
  private final Map<String, String> yamlStringConfigs;
  private final Map<String, List<String>> yamlListConfigs;
  private final ConfigType configType;
  private Command command;
  private final Set onlyDefinedWithCliArgs = ImmutableSet.of(
      CliConstants.VERBOSE);
  private final Framework framework;
  private final BaseParameters parameters;

  private ParametersHolder(CommandLine parsedCommandLine,
      YamlConfigFile yamlConfig, ConfigType configType, Command command)
      throws ParseException, YarnException {
    this.parsedCommandLine = parsedCommandLine;
    this.yamlStringConfigs = initStringConfigValues(yamlConfig);
    this.yamlListConfigs = initListConfigValues(yamlConfig);
    this.configType = configType;
    this.command = command;
    this.framework = determineFrameworkType();
    this.ensureOnlyValidSectionsAreDefined(yamlConfig);
    this.parameters = createParameters();
  }

  private BaseParameters createParameters() {
    if (command == Command.RUN_JOB) {
      if (framework == Framework.TENSORFLOW) {
        return new TensorFlowRunJobParameters();
      } else if (framework == Framework.PYTORCH) {
        return new PyTorchRunJobParameters();
      } else {
        throw new UnsupportedOperationException(SUPPORTED_FRAMEWORKS_MESSAGE);
      }
    } else if (command == Command.SHOW_JOB) {
      return new ShowJobParameters();
    } else if (command == Command.KILL_JOB) {
      return new KillJobParameters();
    } else {
      throw new UnsupportedOperationException(SUPPORTED_COMMANDS_MESSAGE);
    }
  }

  private void ensureOnlyValidSectionsAreDefined(YamlConfigFile yamlConfig) {
    if (isCommandRunJob() && isFrameworkPyTorch() &&
        isPsSectionDefined(yamlConfig)) {
      throw new YamlParseException(
          "PS section should not be defined when PyTorch " +
              "is the selected framework!");
    }

    if (isCommandRunJob() && isFrameworkPyTorch() &&
        isTensorboardSectionDefined(yamlConfig)) {
      throw new YamlParseException(
          "TensorBoard section should not be defined when PyTorch " +
              "is the selected framework!");
    }
  }

  private boolean isCommandRunJob() {
    return command == Command.RUN_JOB;
  }

  private boolean isFrameworkPyTorch() {
    return framework == Framework.PYTORCH;
  }

  private boolean isPsSectionDefined(YamlConfigFile yamlConfig) {
    return yamlConfig != null &&
        yamlConfig.getRoles() != null &&
        yamlConfig.getRoles().getPs() != null;
  }

  private boolean isTensorboardSectionDefined(YamlConfigFile yamlConfig) {
    return yamlConfig != null &&
        yamlConfig.getTensorBoard() != null;
  }

  private Framework determineFrameworkType()
      throws ParseException, YarnException {
    if (!isCommandRunJob()) {
      return null;
    }
    String frameworkStr = getOptionValue(CliConstants.FRAMEWORK);
    if (frameworkStr == null) {
      LOG.info("Framework is not defined in config, falling back to " +
          "TensorFlow as a default.");
      return Framework.TENSORFLOW;
    }
    Framework framework = Framework.parseByValue(frameworkStr);
    if (framework == null) {
      if (getConfigType() == ConfigType.CLI) {
        throw new ParseException("Failed to parse Framework type! "
            + "Valid values are: " + Framework.getValues());
      } else {
        throw new YamlParseException(YAML_PARSE_FAILED +
            ", framework should is defined, but it has an invalid value! " +
            "Valid values are: " + Framework.getValues());
      }
    }
    return framework;
  }

  /**
   * Maps every value coming from the passed yamlConfig to {@code CliConstants}.
   * @param yamlConfig Parsed YAML config
   * @return A map of config values, keys are {@code CliConstants}
   * and values are Strings.
   */
  private Map<String, String> initStringConfigValues(
      YamlConfigFile yamlConfig) {
    if (yamlConfig == null) {
      return Collections.emptyMap();
    }
    Map<String, String> yamlConfigValues = Maps.newHashMap();
    Roles roles = yamlConfig.getRoles();

    initGenericConfigs(yamlConfig, yamlConfigValues);
    initPs(yamlConfigValues, roles.getPs());
    initWorker(yamlConfigValues, roles.getWorker());
    initScheduling(yamlConfigValues, yamlConfig.getScheduling());
    initSecurity(yamlConfigValues, yamlConfig.getSecurity());
    initTensorBoard(yamlConfigValues, yamlConfig.getTensorBoard());

    return yamlConfigValues;
  }

  private Map<String, List<String>> initListConfigValues(
      YamlConfigFile yamlConfig) {
    if (yamlConfig == null) {
      return Collections.emptyMap();
    }

    Map<String, List<String>> yamlConfigValues = Maps.newHashMap();
    Configs configs = yamlConfig.getConfigs();
    yamlConfigValues.put(CliConstants.LOCALIZATION, configs.getLocalizations());
    yamlConfigValues.put(CliConstants.ENV,
        convertToEnvsList(configs.getEnvs()));
    yamlConfigValues.put(CliConstants.QUICKLINK, configs.getQuicklinks());

    return yamlConfigValues;
  }

  private void initGenericConfigs(YamlConfigFile yamlConfig,
      Map<String, String> yamlConfigs) {
    yamlConfigs.put(CliConstants.NAME, yamlConfig.getSpec().getName());
    yamlConfigs.put(CliConstants.FRAMEWORK,
        yamlConfig.getSpec().getFramework());

    Configs configs = yamlConfig.getConfigs();
    yamlConfigs.put(CliConstants.INPUT_PATH, configs.getInputPath());
    yamlConfigs.put(CliConstants.CHECKPOINT_PATH, configs.getCheckpointPath());
    yamlConfigs.put(CliConstants.SAVED_MODEL_PATH, configs.getSavedModelPath());
    yamlConfigs.put(CliConstants.DOCKER_IMAGE, configs.getDockerImage());
    yamlConfigs.put(CliConstants.WAIT_JOB_FINISH, configs.getWaitJobFinish());
  }

  private void initPs(Map<String, String> yamlConfigs, Role ps) {
    if (ps == null) {
      return;
    }
    yamlConfigs.put(CliConstants.N_PS, String.valueOf(ps.getReplicas()));
    yamlConfigs.put(CliConstants.PS_RES, ps.getResources());
    yamlConfigs.put(CliConstants.PS_DOCKER_IMAGE, ps.getDockerImage());
    yamlConfigs.put(CliConstants.PS_LAUNCH_CMD, ps.getLaunchCmd());
  }

  private void initWorker(Map<String, String> yamlConfigs, Role worker) {
    if (worker == null) {
      return;
    }
    yamlConfigs.put(CliConstants.N_WORKERS,
        String.valueOf(worker.getReplicas()));
    yamlConfigs.put(CliConstants.WORKER_RES, worker.getResources());
    yamlConfigs.put(CliConstants.WORKER_DOCKER_IMAGE, worker.getDockerImage());
    yamlConfigs.put(CliConstants.WORKER_LAUNCH_CMD, worker.getLaunchCmd());
  }

  private void initScheduling(Map<String, String> yamlConfigValues,
      Scheduling scheduling) {
    if (scheduling == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.QUEUE, scheduling.getQueue());
  }

  private void initSecurity(Map<String, String> yamlConfigValues,
      Security security) {
    if (security == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.KEYTAB, security.getKeytab());
    yamlConfigValues.put(CliConstants.PRINCIPAL, security.getPrincipal());
    yamlConfigValues.put(CliConstants.DISTRIBUTE_KEYTAB,
        String.valueOf(security.isDistributeKeytab()));
  }

  private void initTensorBoard(Map<String, String> yamlConfigValues,
      TensorBoard tensorBoard) {
    if (tensorBoard == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.TENSORBOARD, Boolean.TRUE.toString());
    yamlConfigValues.put(CliConstants.TENSORBOARD_DOCKER_IMAGE,
        tensorBoard.getDockerImage());
    yamlConfigValues.put(CliConstants.TENSORBOARD_RESOURCES,
        tensorBoard.getResources());
  }

  private List<String> convertToEnvsList(Map<String, String> envs) {
    if (envs == null) {
      return Collections.emptyList();
    }
    return envs.entrySet().stream()
        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  public static ParametersHolder createWithCmdLine(CommandLine cli,
      Command command) throws ParseException, YarnException {
    return new ParametersHolder(cli, null, ConfigType.CLI, command);
  }

  public static ParametersHolder createWithCmdLineAndYaml(CommandLine cli,
      YamlConfigFile yamlConfig, Command command) throws ParseException,
      YarnException {
    return new ParametersHolder(cli, yamlConfig, ConfigType.YAML, command);
  }

  /**
   * Gets the option value, either from the CLI arguments or YAML config,
   * if present.
   * @param option Name of the config.
   * @return The value of the config
   */
  public String getOptionValue(String option) throws YarnException {
    ensureConfigIsDefinedOnce(option, true);
    if (onlyDefinedWithCliArgs.contains(option) ||
        parsedCommandLine.hasOption(option)) {
      return getValueFromCLI(option);
    }
    return getValueFromYaml(option);
  }

  /**
   * Gets the option values, either from the CLI arguments or YAML config,
   * if present.
   * @param option Name of the config.
   * @return The values of the config
   */
  public List<String> getOptionValues(String option) throws YarnException {
    ensureConfigIsDefinedOnce(option, false);
    if (onlyDefinedWithCliArgs.contains(option) ||
        parsedCommandLine.hasOption(option)) {
      return getValuesFromCLI(option);
    }
    return getValuesFromYaml(option);
  }

  private void ensureConfigIsDefinedOnce(String option, boolean stringValue)
      throws YarnException {
    boolean definedWithYaml;
    if (stringValue) {
      definedWithYaml = yamlStringConfigs.containsKey(option);
    } else {
      definedWithYaml = yamlListConfigs.containsKey(option);
    }

    if (parsedCommandLine.hasOption(option) && definedWithYaml) {
      throw new YarnException("Config '%s' is defined both with YAML config" +
          " and with CLI argument, please only use either way!");
    }
  }

  private String getValueFromCLI(String option) {
    String value = parsedCommandLine.getOptionValue(option);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found config value {} for key {} " +
          "from CLI configuration.", value, option);
    }
    return value;
  }

  private List<String> getValuesFromCLI(String option) {
    String[] optionValues = parsedCommandLine.getOptionValues(option);
    if (optionValues != null) {
      List<String> values = Arrays.asList(optionValues);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found config values {} for key {} " +
            "from CLI configuration.", values, option);
      }
      return values;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No config values found for key {} " +
            "from CLI configuration.", option);
      }
      return Lists.newArrayList();
    }
  }

  private String getValueFromYaml(String option) {
    String value = yamlStringConfigs.get(option);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found config value {} for key {} " +
          "from YAML configuration.", value, option);
    }
    return value;
  }

  private List<String> getValuesFromYaml(String option) {
    List<String> values = yamlListConfigs.get(option);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found config values {} for key {} " +
          "from YAML configuration.", values, option);
    }
    return values;
  }

  /**
   * Returns the boolean value of option.
   * First, we check if the CLI value is defined for the option.
   * If not, then we check the YAML value.
   * @param option name of the option
   * @return true, if the option is found in the CLI args or in the YAML config,
   * false otherwise.
   */
  public boolean hasOption(String option) {
    if (onlyDefinedWithCliArgs.contains(option)) {
      boolean value = parsedCommandLine.hasOption(option);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found boolean config with value {} for key {} " +
            "from CLI configuration.", value, option);
      }
      return value;
    }
    if (parsedCommandLine.hasOption(option)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found boolean config value for key {} " +
            "from CLI configuration.", option);
      }
      return true;
    }
    return getBooleanValueFromYaml(option);
  }

  private boolean getBooleanValueFromYaml(String option) {
    String stringValue = yamlStringConfigs.get(option);
    boolean result = stringValue != null
        && Boolean.valueOf(stringValue).equals(Boolean.TRUE);
    LOG.debug("Found config value {} for key {} " +
        "from YAML configuration.", result, option);
    return result;
  }

  public ConfigType getConfigType() {
    return configType;
  }

  public Framework getFramework() {
    return framework;
  }

  public void updateParameters(ClientContext clientContext)
      throws ParseException, YarnException, IOException {
    parameters.updateParameters(this, clientContext);
  }

  public BaseParameters getParameters() {
    return parameters;
  }
}
