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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.ServiceApiConstants;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.ResourceInformation;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.Envs;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

/**
 * Submit a job to cluster
 */
public class YarnServiceJobSubmitter implements JobSubmitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnServiceJobSubmitter.class);
  ClientContext clientContext;
  Service serviceSpec;
  private Set<Path> uploadedFiles = new HashSet<>();

  public YarnServiceJobSubmitter(ClientContext clientContext) {
    this.clientContext = clientContext;
  }

  private Resource getServiceResourceFromYarnResource(
      org.apache.hadoop.yarn.api.records.Resource yarnResource) {
    Resource serviceResource = new Resource();
    serviceResource.setCpus(yarnResource.getVirtualCores());
    serviceResource.setMemory(String.valueOf(yarnResource.getMemorySize()));

    Map<String, ResourceInformation> riMap = new HashMap<>();
    for (org.apache.hadoop.yarn.api.records.ResourceInformation ri : yarnResource
        .getAllResourcesListCopy()) {
      ResourceInformation serviceRi =
          new ResourceInformation();
      serviceRi.setValue(ri.getValue());
      serviceRi.setUnit(ri.getUnits());
      riMap.put(ri.getName(), serviceRi);
    }
    serviceResource.setResourceInformations(riMap);

    return serviceResource;
  }

  private String getValueOfEnvionment(String envar) {
    // extract value from "key=value" form
    if (envar == null || !envar.contains("=")) {
      return "";
    } else {
      return envar.substring(envar.indexOf("=") + 1);
    }
  }

  private void addHdfsClassPathIfNeeded(RunJobParameters parameters,
      FileWriter fw, Component comp) throws IOException {
    // Find envs to use HDFS
    String hdfsHome = null;
    String javaHome = null;

    boolean hadoopEnv = false;

    for (String envar : parameters.getEnvars()) {
      if (envar.startsWith("DOCKER_HADOOP_HDFS_HOME=")) {
        hdfsHome = getValueOfEnvionment(envar);
        hadoopEnv = true;
      } else if (envar.startsWith("DOCKER_JAVA_HOME=")) {
        javaHome = getValueOfEnvionment(envar);
      }
    }

    boolean lackingEnvs = false;

    if ((parameters.getInputPath() != null && parameters.getInputPath()
        .contains("hdfs://")) || (parameters.getCheckpointPath() != null
        && parameters.getCheckpointPath().contains("hdfs://")) || (
        parameters.getSavedModelPath() != null && parameters.getSavedModelPath()
            .contains("hdfs://")) || hadoopEnv) {
      // HDFS is asked either in input or output, set LD_LIBRARY_PATH
      // and classpath

      if (hdfsHome != null) {
        // Unset HADOOP_HOME/HADOOP_YARN_HOME to make sure host machine's envs
        // won't pollute docker's env.
        fw.append("export HADOOP_HOME=\n");
        fw.append("export HADOOP_YARN_HOME=\n");
        fw.append("export HADOOP_HDFS_HOME=" + hdfsHome + "\n");
      } else{
        lackingEnvs = true;
      }

      // hadoop confs will be uploaded to HDFS and localized to container's
      // local folder, so here set $HADOOP_CONF_DIR to $WORK_DIR.
      fw.append("export HADOOP_CONF_DIR=$WORK_DIR\n");
      if (javaHome != null) {
        fw.append("export JAVA_HOME=" + javaHome + "\n");
        fw.append("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"
            + "$JAVA_HOME/lib/amd64/server\n");
      } else {
        lackingEnvs = true;
      }
      fw.append("export CLASSPATH=`$HADOOP_HDFS_HOME/bin/hadoop classpath --glob`\n");
    }

    if (lackingEnvs) {
      LOG.error("When hdfs is being used to read/write models/data. Following"
          + "envs are required: 1) DOCKER_HADOOP_HDFS_HOME=<HDFS_HOME inside"
          + "docker container> 2) DOCKER_JAVA_HOME=<JAVA_HOME inside docker"
          + "container>. You can use --env to pass these envars.");
      throw new IOException("Failed to detect HDFS-related environments.");
    }

    // Trying to upload core-site.xml and hdfs-site.xml
    Path stagingDir =
        clientContext.getRemoteDirectoryManager().getJobStagingArea(
            parameters.getName(), true);
    File coreSite = findFileOnClassPath("core-site.xml");
    File hdfsSite = findFileOnClassPath("hdfs-site.xml");
    if (coreSite == null || hdfsSite == null) {
      LOG.error("hdfs is being used, however we couldn't locate core-site.xml/"
          + "hdfs-site.xml from classpath, please double check you classpath"
          + "setting and make sure they're included.");
      throw new IOException(
          "Failed to locate core-site.xml / hdfs-site.xml from class path");
    }
    uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        coreSite.getAbsolutePath(), "core-site.xml", comp);
    uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        hdfsSite.getAbsolutePath(), "hdfs-site.xml", comp);

    // DEBUG
    if (SubmarineLogs.isVerbose()) {
      fw.append("echo $CLASSPATH\n");
      fw.append("echo $JAVA_HOME\n");
      fw.append("echo $LD_LIBRARY_PATH\n");
      fw.append("echo $HADOOP_HDFS_HOME\n");
    }
  }

  private void addCommonEnvironments(Component component, TaskType taskType) {
    Map<String, String> envs = component.getConfiguration().getEnv();
    envs.put(Envs.TASK_INDEX_ENV, ServiceApiConstants.COMPONENT_ID);
    envs.put(Envs.TASK_TYPE_ENV, taskType.name());
  }

  /*
   * Generate a command launch script on local disk, returns patch to the script
   */
  private String generateCommandLaunchScript(RunJobParameters parameters,
      TaskType taskType, Component comp) throws IOException {
    File file = File.createTempFile(taskType.name() + "-launch-script", ".sh");
    FileWriter fw = new FileWriter(file);

    fw.append("#!/bin/bash\n");

    addHdfsClassPathIfNeeded(parameters, fw, comp);

    // For primary_worker
    if (taskType == TaskType.PRIMARY_WORKER) {
      // Do we need tensorboard?
      if (parameters.isTensorboardEnabled()) {
        int tensorboardPort = 6006;
        // Run tensorboard at the background
        fw.append(
            "tensorboard --port " + tensorboardPort + " --logdir " + parameters
                .getCheckpointPath() + " &\n");
      }
    }

    // When distributed training is required
    if (parameters.isDistributed()) {
      // Generated TF_CONFIG
      String tfConfigEnv = YarnServiceUtils.getTFConfigEnv(
          taskType.getComponentName(), parameters.getNumWorkers(),
          parameters.getNumPS(), parameters.getName(),
          System.getProperty("user.name"),
          clientContext.getYarnConfig().get("hadoop.registry.dns.domain-name"));
      fw.append("export TF_CONFIG=\"" + tfConfigEnv + "\"\n");
    }

    // Print launch command
    if (taskType.equals(TaskType.WORKER) || taskType.equals(
        TaskType.PRIMARY_WORKER)) {
      fw.append(parameters.getWorkerLaunchCmd() + '\n');

      if (SubmarineLogs.isVerbose()) {
        LOG.info("Worker command =[" + parameters.getWorkerLaunchCmd() + "]");
      }
    } else if (taskType.equals(TaskType.PS)) {
      fw.append(parameters.getPSLaunchCmd() + '\n');

      if (SubmarineLogs.isVerbose()) {
        LOG.info("PS command =[" + parameters.getPSLaunchCmd() + "]");
      }
    }

    fw.close();
    return file.getAbsolutePath();
  }

  private String getScriptFileName(TaskType taskType) {
    return "run-" + taskType.name() + ".sh";
  }

  private File findFileOnClassPath(final String fileName) {
    final String classpath = System.getProperty("java.class.path");
    final String pathSeparator = System.getProperty("path.separator");
    final StringTokenizer tokenizer = new StringTokenizer(classpath,
        pathSeparator);

    while (tokenizer.hasMoreTokens()) {
      final String pathElement = tokenizer.nextToken();
      final File directoryOrJar = new File(pathElement);
      final File absoluteDirectoryOrJar = directoryOrJar.getAbsoluteFile();
      if (absoluteDirectoryOrJar.isFile()) {
        final File target = new File(absoluteDirectoryOrJar.getParent(),
            fileName);
        if (target.exists()) {
          return target;
        }
      } else{
        final File target = new File(directoryOrJar, fileName);
        if (target.exists()) {
          return target;
        }
      }
    }

    return null;
  }

  private void uploadToRemoteFileAndLocalizeToContainerWorkDir(Path stagingDir,
      String fileToUpload, String destFilename, Component comp)
      throws IOException {
    FileSystem fs = FileSystem.get(clientContext.getYarnConfig());

    // Upload to remote FS under staging area
    File localFile = new File(fileToUpload);
    if (!localFile.exists()) {
      throw new FileNotFoundException(
          "Trying to upload file=" + localFile.getAbsolutePath()
              + " to remote, but couldn't find local file.");
    }
    String filename = new File(fileToUpload).getName();

    Path uploadedFilePath = new Path(stagingDir, filename);
    if (!uploadedFiles.contains(uploadedFilePath)) {
      if (SubmarineLogs.isVerbose()) {
        LOG.info("Copying local file=" + fileToUpload + " to remote="
            + uploadedFilePath);
      }
      fs.copyFromLocalFile(new Path(fileToUpload), uploadedFilePath);
      uploadedFiles.add(uploadedFilePath);
    }

    FileStatus fileStatus = fs.getFileStatus(uploadedFilePath);
    LOG.info("Uploaded file path = " + fileStatus.getPath());

    // Set it to component's files list
    comp.getConfiguration().getFiles().add(new ConfigFile().srcFile(
        fileStatus.getPath().toUri().toString()).destFile(destFilename)
        .type(ConfigFile.TypeEnum.STATIC));
  }

  private void handleLaunchCommand(RunJobParameters parameters,
      TaskType taskType, Component component) throws IOException {
    // Get staging area directory
    Path stagingDir =
        clientContext.getRemoteDirectoryManager().getJobStagingArea(
            parameters.getName(), true);

    // Generate script file in the local disk
    String localScriptFile = generateCommandLaunchScript(parameters, taskType,
        component);
    String destScriptFileName = getScriptFileName(taskType);
    uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir, localScriptFile,
        destScriptFileName, component);

    component.setLaunchCommand("./" + destScriptFileName);
  }

  private void addWorkerComponent(Service service,
      RunJobParameters parameters, TaskType taskType) throws IOException {
    Component workerComponent = new Component();
    addCommonEnvironments(workerComponent, taskType);

    workerComponent.setName(taskType.getComponentName());

    if (taskType.equals(TaskType.PRIMARY_WORKER)) {
      workerComponent.setNumberOfContainers(1L);
    } else{
      workerComponent.setNumberOfContainers(
          (long) parameters.getNumWorkers() - 1);
    }

    if (parameters.getWorkerDockerImage() != null) {
      workerComponent.setArtifact(
          getDockerArtifact(parameters.getWorkerDockerImage()));
    }

    workerComponent.setResource(
        getServiceResourceFromYarnResource(parameters.getWorkerResource()));
    handleLaunchCommand(parameters, taskType, workerComponent);
    workerComponent.setRestartPolicy(Component.RestartPolicyEnum.NEVER);
    service.addComponent(workerComponent);
  }

  // Handle worker and primary_worker.
  private void addWorkerComponents(Service service, RunJobParameters parameters)
      throws IOException {
    addWorkerComponent(service, parameters, TaskType.PRIMARY_WORKER);

    if (parameters.getNumWorkers() > 1) {
      addWorkerComponent(service, parameters, TaskType.WORKER);
    }
  }

  private void appendToEnv(Service service, String key, String value,
      String delim) {
    Map<String, String> env = service.getConfiguration().getEnv();
    if (!env.containsKey(key)) {
      env.put(key, value);
    } else {
      if (!value.isEmpty()) {
        String existingValue = env.get(key);
        if (!existingValue.endsWith(delim)) {
          env.put(key, existingValue + delim + value);
        } else {
          env.put(key, existingValue + value);
        }
      }
    }
  }

  private void handleServiceEnvs(Service service, RunJobParameters parameters) {
    if (parameters.getEnvars() != null) {
      for (String envarPair : parameters.getEnvars()) {
        String key, value;
        if (envarPair.contains("=")) {
          int idx = envarPair.indexOf('=');
          key = envarPair.substring(0, idx);
          value = envarPair.substring(idx + 1);
        } else{
          // No "=" found so use the whole key
          key = envarPair;
          value = "";
        }
        appendToEnv(service, key, value, ":");
      }
    }

    // Append other configs like /etc/passwd, /etc/krb5.conf
    appendToEnv(service, "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS",
        "/etc/passwd:/etc/passwd:ro", ",");

    String authenication = clientContext.getYarnConfig().get(
        HADOOP_SECURITY_AUTHENTICATION);
    if (authenication != null && authenication.equals("kerberos")) {
      appendToEnv(service, "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS",
          "/etc/krb5.conf:/etc/krb5.conf:ro", ",");
    }
  }

  private Artifact getDockerArtifact(String dockerImageName) {
    return new Artifact().type(Artifact.TypeEnum.DOCKER).id(dockerImageName);
  }

  private Service createServiceByParameters(RunJobParameters parameters)
      throws IOException {
    Service service = new Service();
    service.setName(parameters.getName());
    service.setVersion(String.valueOf(System.currentTimeMillis()));
    service.setArtifact(getDockerArtifact(parameters.getDockerImageName()));

    handleServiceEnvs(service, parameters);

    addWorkerComponents(service, parameters);

    if (parameters.getNumPS() > 0) {
      Component psComponent = new Component();
      psComponent.setName(TaskType.PS.getComponentName());
      addCommonEnvironments(psComponent, TaskType.PS);
      psComponent.setNumberOfContainers((long) parameters.getNumPS());
      psComponent.setRestartPolicy(Component.RestartPolicyEnum.NEVER);
      psComponent.setResource(
          getServiceResourceFromYarnResource(parameters.getPsResource()));

      // Override global docker image if needed.
      if (parameters.getPsDockerImage() != null) {
        psComponent.setArtifact(
            getDockerArtifact(parameters.getPsDockerImage()));
      }
      handleLaunchCommand(parameters, TaskType.PS, psComponent);
      service.addComponent(psComponent);
    }
    return service;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ApplicationId submitJob(RunJobParameters parameters)
      throws IOException, YarnException {
    Service service = createServiceByParameters(parameters);
    ServiceClient serviceClient = YarnServiceUtils.createServiceClient(
        clientContext.getYarnConfig());
    ApplicationId appid = serviceClient.actionCreate(service);
    serviceClient.stop();
    this.serviceSpec = service;
    return appid;
  }

  @VisibleForTesting
  public Service getServiceSpec() {
    return serviceSpec;
  }
}
