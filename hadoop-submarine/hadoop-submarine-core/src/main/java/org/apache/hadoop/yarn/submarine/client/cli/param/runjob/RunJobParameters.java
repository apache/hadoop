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
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param.runjob;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.CliUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.Localization;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.client.cli.param.Quicklink;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.RoleParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.common.resource.ResourceUtils;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters used to run a job
 */
public abstract class RunJobParameters extends RunParameters {
  private String input;
  private String checkpointPath;

  private List<Quicklink> quicklinks = new ArrayList<>();
  private List<Localization> localizations = new ArrayList<>();

  private boolean waitJobFinish = false;
  protected boolean distributed = false;

  private boolean securityDisabled = false;
  private String keytab;
  private String principal;
  private boolean distributeKeytab = false;
  private List<String> confPairs = new ArrayList<>();

  RoleParameters workerParameters =
      RoleParameters.createEmpty(TensorFlowRole.WORKER);

  @Override
  public void updateParameters(ParametersHolder parametersHolder,
      ClientContext clientContext)
      throws ParseException, IOException, YarnException {

    String input = parametersHolder.getOptionValue(CliConstants.INPUT_PATH);
    String jobDir = parametersHolder.getOptionValue(
        CliConstants.CHECKPOINT_PATH);

    if (parametersHolder.hasOption(CliConstants.INSECURE_CLUSTER)) {
      setSecurityDisabled(true);
    }

    String kerberosKeytab = parametersHolder.getOptionValue(
        CliConstants.KEYTAB);
    String kerberosPrincipal = parametersHolder.getOptionValue(
        CliConstants.PRINCIPAL);
    CliUtils.doLoginIfSecure(kerberosKeytab, kerberosPrincipal);

    if (parametersHolder.hasOption(CliConstants.WAIT_JOB_FINISH)) {
      this.waitJobFinish = true;
    }

    // Quicklinks
    List<String> quicklinkStrs = parametersHolder.getOptionValues(
        CliConstants.QUICKLINK);
    if (quicklinkStrs != null) {
      for (String ql : quicklinkStrs) {
        Quicklink quicklink = new Quicklink();
        quicklink.parse(ql);
        quicklinks.add(quicklink);
      }
    }

    // Localizations
    List<String> localizationsStr = parametersHolder.getOptionValues(
        CliConstants.LOCALIZATION);
    if (null != localizationsStr) {
      for (String loc : localizationsStr) {
        Localization localization = new Localization();
        localization.parse(loc);
        localizations.add(localization);
      }
    }
    boolean distributeKerberosKeytab = parametersHolder.hasOption(CliConstants
        .DISTRIBUTE_KEYTAB);

    List<String> configPairs = parametersHolder
        .getOptionValues(CliConstants.ARG_CONF);

    this.setInputPath(input).setCheckpointPath(jobDir)
        .setKeytab(kerberosKeytab)
        .setPrincipal(kerberosPrincipal)
        .setDistributeKeytab(distributeKerberosKeytab)
        .setConfPairs(configPairs);

    super.updateParameters(parametersHolder, clientContext);
  }

  abstract void executePostOperations(ClientContext clientContext)
      throws IOException;

  void setDefaultDirs(ClientContext clientContext) throws IOException {
    // Create directories if needed
    String jobDir = getCheckpointPath();
    if (jobDir == null) {
      jobDir = getJobDir(clientContext);
      setCheckpointPath(jobDir);
    }

    if (getNumWorkers() > 0) {
      String savedModelDir = getSavedModelPath();
      if (savedModelDir == null) {
        savedModelDir = jobDir;
        setSavedModelPath(savedModelDir);
      }
    }
  }

  private String getJobDir(ClientContext clientContext) throws IOException {
    RemoteDirectoryManager rdm = clientContext.getRemoteDirectoryManager();
    if (getNumWorkers() > 0) {
      return rdm.getJobCheckpointDir(getName(), true).toString();
    } else {
      // when #workers == 0, it means we only launch TB. In that case,
      // point job dir to root dir so all job's metrics will be shown.
      return rdm.getUserRootFolder().toString();
    }
  }

  public abstract List<String> getLaunchCommands();

  public String getInputPath() {
    return input;
  }

  public RunJobParameters setInputPath(String input) {
    this.input = input;
    return this;
  }

  public String getCheckpointPath() {
    return checkpointPath;
  }

  public RunJobParameters setCheckpointPath(String checkpointPath) {
    this.checkpointPath = checkpointPath;
    return this;
  }

  public boolean isWaitJobFinish() {
    return waitJobFinish;
  }

  public List<Quicklink> getQuicklinks() {
    return quicklinks;
  }

  public List<Localization> getLocalizations() {
    return localizations;
  }

  public String getKeytab() {
    return keytab;
  }

  public RunJobParameters setKeytab(String kerberosKeytab) {
    this.keytab = kerberosKeytab;
    return this;
  }

  public String getPrincipal() {
    return principal;
  }

  public RunJobParameters setPrincipal(String kerberosPrincipal) {
    this.principal = kerberosPrincipal;
    return this;
  }

  public boolean isSecurityDisabled() {
    return securityDisabled;
  }

  public void setSecurityDisabled(boolean securityDisabled) {
    this.securityDisabled = securityDisabled;
  }

  public boolean isDistributeKeytab() {
    return distributeKeytab;
  }

  public RunJobParameters setDistributeKeytab(
      boolean distributeKerberosKeytab) {
    this.distributeKeytab = distributeKerberosKeytab;
    return this;
  }

  public List<String> getConfPairs() {
    return confPairs;
  }

  public RunJobParameters setConfPairs(List<String> confPairs) {
    this.confPairs = confPairs;
    return this;
  }

  public void setDistributed(boolean distributed) {
    this.distributed = distributed;
  }

  RoleParameters getWorkerParameters(ClientContext clientContext,
      ParametersHolder parametersHolder, String input)
      throws ParseException, YarnException, IOException {
    int nWorkers = getNumberOfWorkers(parametersHolder, input);
    Resource workerResource =
        determineWorkerResource(parametersHolder, nWorkers, clientContext);
    String workerDockerImage =
        parametersHolder.getOptionValue(CliConstants.WORKER_DOCKER_IMAGE);
    String workerLaunchCmd =
        parametersHolder.getOptionValue(CliConstants.WORKER_LAUNCH_CMD);
    return new RoleParameters(TensorFlowRole.WORKER, nWorkers,
        workerLaunchCmd, workerDockerImage, workerResource);
  }

  private Resource determineWorkerResource(ParametersHolder parametersHolder,
      int nWorkers, ClientContext clientContext)
      throws ParseException, YarnException, IOException {
    if (nWorkers > 0) {
      String workerResourceStr =
          parametersHolder.getOptionValue(CliConstants.WORKER_RES);
      if (workerResourceStr == null) {
        throw new ParseException(
            "--" + CliConstants.WORKER_RES + " is absent.");
      }
      return ResourceUtils.createResourceFromString(workerResourceStr);
    }
    return null;
  }

  private int getNumberOfWorkers(ParametersHolder parametersHolder,
      String input) throws ParseException, YarnException {
    int nWorkers = 1;
    if (parametersHolder.getOptionValue(CliConstants.N_WORKERS) != null) {
      nWorkers = Integer
          .parseInt(parametersHolder.getOptionValue(CliConstants.N_WORKERS));
      // Only check null value.
      // Training job shouldn't ignore INPUT_PATH option
      // But if nWorkers is 0, INPUT_PATH can be ignored because
      // user can only run Tensorboard
      if (null == input && 0 != nWorkers) {
        throw new ParseException(
            "\"--" + CliConstants.INPUT_PATH + "\" is absent");
      }
    }
    return nWorkers;
  }

  public String getWorkerLaunchCmd() {
    return workerParameters.getLaunchCommand();
  }

  public void setWorkerLaunchCmd(String launchCmd) {
    workerParameters.setLaunchCommand(launchCmd);
  }

  public int getNumWorkers() {
    return workerParameters.getReplicas();
  }

  public void setNumWorkers(int numWorkers) {
    workerParameters.setReplicas(numWorkers);
  }

  public Resource getWorkerResource() {
    return workerParameters.getResource();
  }

  public void setWorkerResource(Resource resource) {
    workerParameters.setResource(resource);
  }

  public String getWorkerDockerImage() {
    return workerParameters.getDockerImage();
  }

  public void setWorkerDockerImage(String image) {
    workerParameters.setDockerImage(image);
  }

  public boolean isDistributed() {
    return distributed;
  }

  @VisibleForTesting
  public static class UnderscoreConverterPropertyUtils extends PropertyUtils {
    @Override
    public Property getProperty(Class<? extends Object> type, String name)
        throws IntrospectionException {
      if (name.indexOf('_') > -1) {
        name = convertName(name);
      }
      return super.getProperty(type, name);
    }

    private static String convertName(String name) {
      return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
    }
  }
}
