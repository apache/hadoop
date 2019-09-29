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

package org.apache.hadoop.yarn.submarine.client.cli.runjob;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.AbstractCli;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.CliUtils;
import org.apache.hadoop.yarn.submarine.client.cli.Command;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters.UnderscoreConverterPropertyUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlConfigFile;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlParseException;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.exception.SubmarineException;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This purpose of this class is to handle / parse CLI arguments related to
 * the run job Submarine command.
 */
public class RunJobCli extends AbstractCli {
  private static final Logger LOG =
      LoggerFactory.getLogger(RunJobCli.class);

  private static final String TENSORFLOW = "TensorFlow";
  private static final String PYTORCH = "PyTorch";
  private static final String PS = "PS";
  private static final String WORKER = "worker";
  private static final String TENSORBOARD = "TensorBoard";

  private static final String CAN_BE_USED_WITH_TF_PYTORCH =
      String.format("Can be used with %s or %s frameworks.",
          TENSORFLOW, PYTORCH);
  private static final String TENSORFLOW_ONLY =
      String.format("Can only be used with %s framework.", TENSORFLOW);
  public static final String YAML_PARSE_FAILED = "Failed to parse " +
      "YAML config";
  private static final String LOCAL_OR_ANY_FS_DIRECTORY = "Could be a local " +
      "directory or any other directory on the file system.";

  private Options options;
  private JobSubmitter jobSubmitter;
  private JobMonitor jobMonitor;
  private ParametersHolder parametersHolder;

  public RunJobCli(ClientContext cliContext) {
    this(cliContext, cliContext.getRuntimeFactory().getJobSubmitterInstance(),
        cliContext.getRuntimeFactory().getJobMonitorInstance());
  }

  @VisibleForTesting
  public RunJobCli(ClientContext cliContext, JobSubmitter jobSubmitter,
      JobMonitor jobMonitor) {
    super(cliContext);
    this.options = generateOptions();
    this.jobSubmitter = jobSubmitter;
    this.jobMonitor = jobMonitor;
  }

  public void printUsages() {
    new HelpFormatter().printHelp("job run", options);
  }

  private Options generateOptions() {
    Options options = new Options();
    options.addOption(CliConstants.YAML_CONFIG, true,
        "Config file (in YAML format)");
    options.addOption(CliConstants.FRAMEWORK, true,
        String.format("Framework to use. Valid values are: %s! " +
                "The default framework is Tensorflow.",
            Framework.getValues()));
    options.addOption(CliConstants.NAME, true, "Name of the job");
    options.addOption(CliConstants.INPUT_PATH, true,
        "Input of the job. " + LOCAL_OR_ANY_FS_DIRECTORY);
    options.addOption(CliConstants.CHECKPOINT_PATH, true,
        "Training output directory of the job. " + LOCAL_OR_ANY_FS_DIRECTORY +
            "This typically includes checkpoint files and exported model");
    options.addOption(CliConstants.SAVED_MODEL_PATH, true,
        "Model exported path (saved model) of the job, which is needed when " +
            "exported model is not placed under ${checkpoint_path}. " +
            LOCAL_OR_ANY_FS_DIRECTORY + "This will be used to serve");
    options.addOption(CliConstants.DOCKER_IMAGE, true, "Docker image name/tag");
    options.addOption(CliConstants.PS_DOCKER_IMAGE, true,
        getDockerImageMessage(PS));
    options.addOption(CliConstants.WORKER_DOCKER_IMAGE, true,
        getDockerImageMessage(WORKER));
    options.addOption(CliConstants.QUEUE, true,
        "Name of queue to run the job. By default, the default queue is used");

    addWorkerOptions(options);
    addPSOptions(options);
    addTensorboardOptions(options);

    options.addOption(CliConstants.ENV, true,
        "Common environment variable passed to worker / PS");
    options.addOption(CliConstants.VERBOSE, false,
        "Print verbose log for troubleshooting");
    options.addOption(CliConstants.WAIT_JOB_FINISH, false,
        "Specified when user wants to wait for jobs to finish");
    options.addOption(CliConstants.QUICKLINK, true, "Specify quicklink so YARN "
        + "web UI shows link to the given role instance and port. " +
        "When --tensorboard is specified, quicklink to the " +
        TENSORBOARD + " instance will be added automatically. " +
        "The format of quick link is: "
        + "Quick_link_label=http(or https)://role-name:port. " +
        "For example, if users want to link to the first worker's 7070 port, " +
        "and text of quicklink is Notebook_UI, " +
        "users need to specify --quicklink Notebook_UI=https://master-0:7070");
    options.addOption(CliConstants.LOCALIZATION, true, "Specify"
        + " localization to make remote/local file/directory available to"
        + " all container(Docker)."
        + " Argument format is: \"RemoteUri:LocalFilePath[:rw] \" "
        + "(ro permission is not supported yet)."
        + " The RemoteUri can be a local file or directory on the filesystem."
        + " Alternatively, the following remote file systems / "
        + "transmit mechanisms can be used: "
        + " HDFS, S3 or abfs, HTTP, etc."
        + " The LocalFilePath can be absolute or relative."
        + " If it is a relative path, it will be"
        + " under container's implied working directory"
        + " but sub-directory is not supported yet."
        + " This option can be set multiple times."
        + " Examples are \n"
        + "-localization \"hdfs:///user/yarn/mydir2:/opt/data\"\n"
        + "-localization \"s3a:///a/b/myfile1:./\"\n"
        + "-localization \"https:///a/b/myfile2:./myfile\"\n"
        + "-localization \"/user/yarn/mydir3:/opt/mydir3\"\n"
        + "-localization \"./mydir1:.\"\n");
    options.addOption(CliConstants.KEYTAB, true, "Specify keytab used by the " +
        "job under a secured environment");
    options.addOption(CliConstants.PRINCIPAL, true, "Specify principal used " +
        "by the job under a secured environment");
    options.addOption(CliConstants.DISTRIBUTE_KEYTAB, false, "Distribute " +
        "local keytab to cluster machines for service authentication. " +
        "If not specified, pre-distributed keytab of which path specified by" +
        " parameter" + CliConstants.KEYTAB + " on cluster machines will be " +
        "used");
    options.addOption("h", "help", false, "Print help");
    options.addOption("insecure", false, "Cluster is not Kerberos enabled.");
    options.addOption("conf", true,
        "User specified configuration, as key=val pairs.");
    return options;
  }

  private void addWorkerOptions(Options options) {
    options.addOption(CliConstants.N_WORKERS, true,
        getNumberOfServiceMessage(WORKER, 1) +
            CAN_BE_USED_WITH_TF_PYTORCH);
    options.addOption(CliConstants.WORKER_DOCKER_IMAGE, true,
        getDockerImageMessage(WORKER) +
            CAN_BE_USED_WITH_TF_PYTORCH);
    options.addOption(CliConstants.WORKER_LAUNCH_CMD, true,
        getLaunchCommandMessage(WORKER) +
            CAN_BE_USED_WITH_TF_PYTORCH);
    options.addOption(CliConstants.WORKER_RES, true,
        getServiceResourceMessage(WORKER) +
            CAN_BE_USED_WITH_TF_PYTORCH);
  }

  private void addPSOptions(Options options) {
    options.addOption(CliConstants.N_PS, true,
        getNumberOfServiceMessage("PS", 0) +
            TENSORFLOW_ONLY);
    options.addOption(CliConstants.PS_DOCKER_IMAGE, true,
        getDockerImageMessage(PS) +
            TENSORFLOW_ONLY);
    options.addOption(CliConstants.PS_LAUNCH_CMD, true,
        getLaunchCommandMessage("PS") +
            TENSORFLOW_ONLY);
    options.addOption(CliConstants.PS_RES, true,
        getServiceResourceMessage("PS") +
            TENSORFLOW_ONLY);
  }

  private void addTensorboardOptions(Options options) {
    options.addOption(CliConstants.TENSORBOARD, false,
        "Should we run TensorBoard for this job? " +
            "By default, TensorBoard is disabled." +
            TENSORFLOW_ONLY);
    options.addOption(CliConstants.TENSORBOARD_RESOURCES, true,
        "Specifies resources of Tensorboard. The default resource is: "
            + CliConstants.TENSORBOARD_DEFAULT_RESOURCES + "." +
            TENSORFLOW_ONLY);
    options.addOption(CliConstants.TENSORBOARD_DOCKER_IMAGE, true,
        getDockerImageMessage(TENSORBOARD));
  }

  private String getLaunchCommandMessage(String service) {
    return String.format("Launch command of the %s, arguments will be "
        + "directly used to launch the %s", service, service);
  }

  private String getServiceResourceMessage(String serviceType) {
    return String.format("Resource of each %s process, for example: "
        + "memory-mb=2048,vcores=2,yarn.io/gpu=2", serviceType);
  }

  private String getNumberOfServiceMessage(String serviceType,
      int defaultValue) {
    return String.format("Number of %s processes for the job. " +
        "The default value is %d.", serviceType, defaultValue);
  }

  private String getDockerImageMessage(String serviceType) {
    return String.format("Specifies docker image for the %s process. " +
            "When not specified, %s uses --%s as a default value.",
        serviceType, serviceType, CliConstants.DOCKER_IMAGE);
  }

  private void parseCommandLineAndGetRunJobParameters(String[] args)
      throws ParseException, IOException, YarnException {
    try {
      GnuParser parser = new GnuParser();
      CommandLine cli = parser.parse(options, args);
      parametersHolder = createParametersHolder(cli);
      parametersHolder.updateParameters(clientContext);
    } catch (ParseException e) {
      LOG.error("Exception in parse: {}", e.getMessage());
      printUsages();
      throw e;
    }
  }

  private ParametersHolder createParametersHolder(CommandLine cli)
      throws ParseException, YarnException {
    String yamlConfigFile =
        cli.getOptionValue(CliConstants.YAML_CONFIG);
    if (yamlConfigFile != null) {
      YamlConfigFile yamlConfig = readYamlConfigFile(yamlConfigFile);
      checkYamlConfig(yamlConfigFile, yamlConfig);
      LOG.info("Using YAML configuration!");
      return ParametersHolder.createWithCmdLineAndYaml(cli, yamlConfig,
          Command.RUN_JOB);
    } else {
      LOG.info("Using CLI configuration!");
      return ParametersHolder.createWithCmdLine(cli, Command.RUN_JOB);
    }
  }

  private void checkYamlConfig(String yamlConfigFile,
      YamlConfigFile yamlConfig) {
    if (yamlConfig == null) {
      throw new YamlParseException(String.format(
          YAML_PARSE_FAILED + ", file is empty: %s", yamlConfigFile));
    } else if (yamlConfig.getConfigs() == null) {
      throw new YamlParseException(String.format(YAML_PARSE_FAILED +
          ", config section should be defined, but it cannot be found in " +
          "YAML file '%s'!", yamlConfigFile));
    }
  }

  private YamlConfigFile readYamlConfigFile(String filename) {
    Constructor constructor = new Constructor(YamlConfigFile.class);
    constructor.setPropertyUtils(new UnderscoreConverterPropertyUtils());
    try {
      LOG.info("Reading YAML configuration from file: {}", filename);
      Yaml yaml = new Yaml(constructor);
      return yaml.loadAs(FileUtils.openInputStream(new File(filename)),
          YamlConfigFile.class);
    } catch (FileNotFoundException e) {
      logExceptionOfYamlParse(filename, e);
      throw new YamlParseException(YAML_PARSE_FAILED +
          ", file does not exist!");
    } catch (Exception e) {
      logExceptionOfYamlParse(filename, e);
      throw new YamlParseException(
          String.format(YAML_PARSE_FAILED + ", details: %s", e.getMessage()));
    }
  }

  private void logExceptionOfYamlParse(String filename, Exception e) {
    LOG.error(String.format("Exception while parsing YAML file %s", filename),
        e);
  }

  private void storeJobInformation(RunJobParameters parameters,
      ApplicationId applicationId, String[] args) throws IOException {
    String jobName = parameters.getName();
    Map<String, String> jobInfo = new HashMap<>();
    jobInfo.put(StorageKeyConstants.JOB_NAME, jobName);
    jobInfo.put(StorageKeyConstants.APPLICATION_ID, applicationId.toString());

    if (parameters.getCheckpointPath() != null) {
      jobInfo.put(StorageKeyConstants.CHECKPOINT_PATH,
          parameters.getCheckpointPath());
    }
    if (parameters.getInputPath() != null) {
      jobInfo.put(StorageKeyConstants.INPUT_PATH,
          parameters.getInputPath());
    }
    if (parameters.getSavedModelPath() != null) {
      jobInfo.put(StorageKeyConstants.SAVED_MODEL_PATH,
          parameters.getSavedModelPath());
    }

    String joinedArgs = String.join(" ", args);
    jobInfo.put(StorageKeyConstants.JOB_RUN_ARGS, joinedArgs);
    clientContext.getRuntimeFactory().getSubmarineStorage().addNewJob(jobName,
        jobInfo);
  }

  @Override
  public int run(String[] args)
      throws ParseException, IOException, YarnException, SubmarineException {
    if (CliUtils.argsForHelp(args)) {
      printUsages();
      return 0;
    }

    parseCommandLineAndGetRunJobParameters(args);
    ApplicationId applicationId = jobSubmitter.submitJob(parametersHolder);
    RunJobParameters parameters =
        (RunJobParameters) parametersHolder.getParameters();
    storeJobInformation(parameters, applicationId, args);
    if (parameters.isWaitJobFinish()) {
      this.jobMonitor.waitTrainingFinal(parameters.getName());
    }

    return 0;
  }

  @VisibleForTesting
  public JobSubmitter getJobSubmitter() {
    return jobSubmitter;
  }

  @VisibleForTesting
  public RunJobParameters getRunJobParameters() {
    return (RunJobParameters) parametersHolder.getParameters();
  }
}
