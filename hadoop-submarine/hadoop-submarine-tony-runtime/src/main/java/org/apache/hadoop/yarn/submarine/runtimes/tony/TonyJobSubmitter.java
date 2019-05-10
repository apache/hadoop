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

package org.apache.hadoop.yarn.submarine.runtimes.tony;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.client.CallbackHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.Framework;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Implementation of JobSumitter with TonY runtime.
 */
public class TonyJobSubmitter implements JobSubmitter, CallbackHandler {

  private static final Log LOG = LogFactory.getLog(TonyJobSubmitter.class);
  private ApplicationId applicationId;
  private TonyClient tonyClient;

  public TonyJobSubmitter() { }
  public void setTonyClient(TonyClient client) {
    this.tonyClient = client;
  }

  @Override
  public ApplicationId submitJob(ParametersHolder parameters)
      throws IOException {
    if (parameters.getFramework() == Framework.PYTORCH) {
      // we need to throw an exception, as ParametersHolder's parameters field
      // could not be casted to TensorFlowRunJobParameters.
      throw new UnsupportedOperationException(
          "Support \"–-framework\" option for PyTorch in Tony is coming. " +
              "Please check the documentation about how to submit a " +
              "PyTorch job with TonY runtime.");
    }

    LOG.info("Starting Tony runtime..");

    File tonyFinalConfPath = File.createTempFile("temp",
        Constants.TONY_FINAL_XML);
    // Write user's overridden conf to an xml to be localized.
    Configuration tonyConf = TonyUtils.tonyConfFromClientContext(
        (TensorFlowRunJobParameters) parameters.getParameters());
    try (OutputStream os = new FileOutputStream(tonyFinalConfPath)) {
      tonyConf.writeXml(os);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create " + tonyFinalConfPath
          + " conf file. Exiting.", e);
    }

    try {
      tonyClient.init(new String[]{
          "--conf_file", tonyFinalConfPath.getAbsolutePath()
      });
    } catch (Exception e) {
      LOG.error("Failed to init TonyClient: ", e);
    }
    Thread clientThread = new Thread(tonyClient::start);
    java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        tonyClient.forceKillApplication();
      } catch (YarnException | IOException e) {
        LOG.error("Failed to kill application during shutdown.", e);
      }
    }));
    clientThread.start();
    while (clientThread.isAlive()) {
      if (applicationId != null) {
        LOG.info("TonyClient returned applicationId: " + applicationId);
        return applicationId;
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }
    return null;
  }

  @Override
  public void onApplicationIdReceived(ApplicationId appId) {
    applicationId = appId;
  }
}
