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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.param.Quicklink;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.Framework;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.PyTorchRole;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.utils.KerberosPrincipalFactory;
import org.apache.hadoop.yarn.submarine.utils.Localizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getDNSDomain;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getUserName;
import static org.apache.hadoop.yarn.submarine.utils.DockerUtilities.getDockerArtifact;
import static org.apache.hadoop.yarn.submarine.utils.EnvironmentUtilities.handleServiceEnvs;

/**
 * Abstract base class that supports creating service specs for Native Service.
 */
public abstract class AbstractServiceSpec implements ServiceSpec {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractServiceSpec.class);
  protected final RunJobParameters parameters;
  protected final FileSystemOperations fsOperations;
  private final Localizer localizer;
  protected final RemoteDirectoryManager remoteDirectoryManager;
  protected final Configuration yarnConfig;
  protected final LaunchCommandFactory launchCommandFactory;
  private final WorkerComponentFactory workerFactory;

  public AbstractServiceSpec(RunJobParameters parameters,
      ClientContext clientContext, FileSystemOperations fsOperations,
      LaunchCommandFactory launchCommandFactory,
      Localizer localizer) {
    this.parameters = parameters;
    this.remoteDirectoryManager = clientContext.getRemoteDirectoryManager();
    this.yarnConfig = clientContext.getYarnConfig();
    this.fsOperations = fsOperations;
    this.localizer = localizer;
    this.launchCommandFactory = launchCommandFactory;
    this.workerFactory = new WorkerComponentFactory(fsOperations,
        remoteDirectoryManager, parameters, launchCommandFactory, yarnConfig);
  }

  protected ServiceWrapper createServiceSpecWrapper() throws IOException {
    Service serviceSpec = new Service();
    serviceSpec.setName(parameters.getName());
    serviceSpec.setVersion(String.valueOf(System.currentTimeMillis()));
    serviceSpec.setArtifact(getDockerArtifact(parameters.getDockerImageName()));

    KerberosPrincipal kerberosPrincipal = KerberosPrincipalFactory
        .create(fsOperations, remoteDirectoryManager, parameters);
    if (kerberosPrincipal != null) {
      serviceSpec.setKerberosPrincipal(kerberosPrincipal);
    }

    handleServiceEnvs(serviceSpec, yarnConfig, parameters.getEnvars());
    localizer.handleLocalizations(serviceSpec);
    return new ServiceWrapper(serviceSpec);
  }


  // Handle worker and primary_worker.
  protected void addWorkerComponents(ServiceWrapper serviceWrapper,
      Framework framework)
      throws IOException {
    final Role primaryWorkerRole;
    final Role workerRole;
    if (framework == Framework.TENSORFLOW) {
      primaryWorkerRole = TensorFlowRole.PRIMARY_WORKER;
      workerRole = TensorFlowRole.WORKER;
    } else {
      primaryWorkerRole = PyTorchRole.PRIMARY_WORKER;
      workerRole = PyTorchRole.WORKER;
    }

    addWorkerComponent(serviceWrapper, primaryWorkerRole, framework);

    if (parameters.getNumWorkers() > 1) {
      addWorkerComponent(serviceWrapper, workerRole, framework);
    }
  }
  private void addWorkerComponent(ServiceWrapper serviceWrapper,
      Role role, Framework framework) throws IOException {
    AbstractComponent component = workerFactory.create(framework, role);
    serviceWrapper.addComponent(component);
  }

  protected void handleQuicklinks(Service serviceSpec)
      throws IOException {
    List<Quicklink> quicklinks = parameters.getQuicklinks();
    if (quicklinks != null && !quicklinks.isEmpty()) {
      for (Quicklink ql : quicklinks) {
        // Make sure it is a valid instance name
        String instanceName = ql.getComponentInstanceName();
        boolean found = false;

        for (Component comp : serviceSpec.getComponents()) {
          for (int i = 0; i < comp.getNumberOfContainers(); i++) {
            String possibleInstanceName = comp.getName() + "-" + i;
            if (possibleInstanceName.equals(instanceName)) {
              found = true;
              break;
            }
          }
        }

        if (!found) {
          throw new IOException(
              "Couldn't find a component instance = " + instanceName
                  + " while adding quicklink");
        }

        String link = ql.getProtocol()
            + YarnServiceUtils.getDNSName(serviceSpec.getName(), instanceName,
            getUserName(), getDNSDomain(yarnConfig), ql.getPort());
        addQuicklink(serviceSpec, ql.getLabel(), link);
      }
    }
  }

  protected static void addQuicklink(Service serviceSpec, String label,
      String link) {
    Map<String, String> quicklinks = serviceSpec.getQuicklinks();
    if (quicklinks == null) {
      quicklinks = new HashMap<>();
      serviceSpec.setQuicklinks(quicklinks);
    }

    if (SubmarineLogs.isVerbose()) {
      LOG.info("Added quicklink, " + label + "=" + link);
    }

    quicklinks.put(label, link);
  }
}
