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

package org.apache.hadoop.yarn.submarine.client.cli.param;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.CliUtils;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parameters used to run a job
 */
public class RunJobParameters extends RunParameters {
  private String input;
  private String checkpointPath;

  private int numWorkers;
  private int numPS;
  private Resource workerResource;
  private Resource psResource;
  private boolean tensorboardEnabled;
  private Resource tensorboardResource;
  private String tensorboardDockerImage;
  private String workerLaunchCmd;
  private String psLaunchCmd;
  private List<Quicklink> quicklinks = new ArrayList<>();
  private List<Localization> localizations = new ArrayList<>();

  private String psDockerImage = null;
  private String workerDockerImage = null;

  private boolean waitJobFinish = false;
  private boolean distributed = false;

  private String keytab;
  private String principal;
  private boolean distributeKeytab = false;

  @Override
  public void updateParameters(ParametersHolder parametersHolder,
      ClientContext clientContext)
      throws ParseException, IOException, YarnException {

    String input = parametersHolder.getOptionValue(CliConstants.INPUT_PATH);
    String jobDir = parametersHolder.getOptionValue(
        CliConstants.CHECKPOINT_PATH);
    int nWorkers = 1;
    if (parametersHolder.getOptionValue(CliConstants.N_WORKERS) != null) {
      nWorkers = Integer.parseInt(
          parametersHolder.getOptionValue(CliConstants.N_WORKERS));
      // Only check null value.
      // Training job shouldn't ignore INPUT_PATH option
      // But if nWorkers is 0, INPUT_PATH can be ignored because
      // user can only run Tensorboard
      if (null == input && 0 != nWorkers) {
        throw new ParseException("\"--" + CliConstants.INPUT_PATH +
            "\" is absent");
      }
    }

    int nPS = 0;
    if (parametersHolder.getOptionValue(CliConstants.N_PS) != null) {
      nPS = Integer.parseInt(
          parametersHolder.getOptionValue(CliConstants.N_PS));
    }

    // Check #workers and #ps.
    // When distributed training is required
    if (nWorkers >= 2 && nPS > 0) {
      distributed = true;
    } else if (nWorkers <= 1 && nPS > 0) {
      throw new ParseException("Only specified one worker but non-zero PS, "
          + "please double check.");
    }

    String kerberosKeytab = parametersHolder.getOptionValue(
        CliConstants.KEYTAB);
    String kerberosPrincipal = parametersHolder.getOptionValue(
        CliConstants.PRINCIPAL);
    CliUtils.doLoginIfSecure(kerberosKeytab, kerberosPrincipal);

    workerResource = null;
    if (nWorkers > 0) {
      String workerResourceStr = parametersHolder.getOptionValue(
          CliConstants.WORKER_RES);
      if (workerResourceStr == null) {
        throw new ParseException(
            "--" + CliConstants.WORKER_RES + " is absent.");
      }
      workerResource = ResourceUtils.createResourceFromString(
          workerResourceStr,
          clientContext.getOrCreateYarnClient().getResourceTypeInfo());
    }

    Resource psResource = null;
    if (nPS > 0) {
      String psResourceStr = parametersHolder.getOptionValue(
          CliConstants.PS_RES);
      if (psResourceStr == null) {
        throw new ParseException("--" + CliConstants.PS_RES + " is absent.");
      }
      psResource = ResourceUtils.createResourceFromString(psResourceStr,
          clientContext.getOrCreateYarnClient().getResourceTypeInfo());
    }

    boolean tensorboard = false;
    if (parametersHolder.hasOption(CliConstants.TENSORBOARD)) {
      tensorboard = true;
      String tensorboardResourceStr = parametersHolder.getOptionValue(
          CliConstants.TENSORBOARD_RESOURCES);
      if (tensorboardResourceStr == null || tensorboardResourceStr.isEmpty()) {
        tensorboardResourceStr = CliConstants.TENSORBOARD_DEFAULT_RESOURCES;
      }
      tensorboardResource = ResourceUtils.createResourceFromString(
          tensorboardResourceStr,
          clientContext.getOrCreateYarnClient().getResourceTypeInfo());
      tensorboardDockerImage = parametersHolder.getOptionValue(
          CliConstants.TENSORBOARD_DOCKER_IMAGE);
      this.setTensorboardResource(tensorboardResource);
    }

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

    psDockerImage = parametersHolder.getOptionValue(
        CliConstants.PS_DOCKER_IMAGE);
    workerDockerImage = parametersHolder.getOptionValue(
        CliConstants.WORKER_DOCKER_IMAGE);

    String workerLaunchCmd = parametersHolder.getOptionValue(
        CliConstants.WORKER_LAUNCH_CMD);
    String psLaunchCommand = parametersHolder.getOptionValue(
        CliConstants.PS_LAUNCH_CMD);

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

    this.setInputPath(input).setCheckpointPath(jobDir)
        .setNumPS(nPS).setNumWorkers(nWorkers)
        .setPSLaunchCmd(psLaunchCommand).setWorkerLaunchCmd(workerLaunchCmd)
        .setPsResource(psResource)
        .setTensorboardEnabled(tensorboard)
        .setKeytab(kerberosKeytab)
        .setPrincipal(kerberosPrincipal)
        .setDistributeKeytab(distributeKerberosKeytab);

    super.updateParameters(parametersHolder, clientContext);
  }

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

  public int getNumWorkers() {
    return numWorkers;
  }

  public RunJobParameters setNumWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
    return this;
  }

  public int getNumPS() {
    return numPS;
  }

  public RunJobParameters setNumPS(int numPS) {
    this.numPS = numPS;
    return this;
  }

  public Resource getWorkerResource() {
    return workerResource;
  }

  public RunJobParameters setWorkerResource(Resource workerResource) {
    this.workerResource = workerResource;
    return this;
  }

  public Resource getPsResource() {
    return psResource;
  }

  public RunJobParameters setPsResource(Resource psResource) {
    this.psResource = psResource;
    return this;
  }

  public boolean isTensorboardEnabled() {
    return tensorboardEnabled;
  }

  public RunJobParameters setTensorboardEnabled(boolean tensorboardEnabled) {
    this.tensorboardEnabled = tensorboardEnabled;
    return this;
  }

  public String getWorkerLaunchCmd() {
    return workerLaunchCmd;
  }

  public RunJobParameters setWorkerLaunchCmd(String workerLaunchCmd) {
    this.workerLaunchCmd = workerLaunchCmd;
    return this;
  }

  public String getPSLaunchCmd() {
    return psLaunchCmd;
  }

  public RunJobParameters setPSLaunchCmd(String psLaunchCmd) {
    this.psLaunchCmd = psLaunchCmd;
    return this;
  }

  public boolean isWaitJobFinish() {
    return waitJobFinish;
  }


  public String getPsDockerImage() {
    return psDockerImage;
  }

  public String getWorkerDockerImage() {
    return workerDockerImage;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public Resource getTensorboardResource() {
    return tensorboardResource;
  }

  public void setTensorboardResource(Resource tensorboardResource) {
    this.tensorboardResource = tensorboardResource;
  }

  public String getTensorboardDockerImage() {
    return tensorboardDockerImage;
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

  public boolean isDistributeKeytab() {
    return distributeKeytab;
  }

  public RunJobParameters setDistributeKeytab(
      boolean distributeKerberosKeytab) {
    this.distributeKeytab = distributeKerberosKeytab;
    return this;
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
