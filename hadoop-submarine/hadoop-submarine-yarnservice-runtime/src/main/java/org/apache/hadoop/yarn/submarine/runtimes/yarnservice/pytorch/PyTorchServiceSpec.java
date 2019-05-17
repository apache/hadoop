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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.pytorch;

import java.io.IOException;

import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.Framework;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.AbstractServiceSpec;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.ServiceWrapper;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.PyTorchLaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.utils.Localizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains all the logic to create an instance
 * of a {@link Service} object for PyTorch.
 * Please note that currently, only single-node (non-distributed)
 * support is implemented for PyTorch.
 */
public class PyTorchServiceSpec extends AbstractServiceSpec {
  private static final Logger LOG =
      LoggerFactory.getLogger(PyTorchServiceSpec.class);
  //this field is needed in the future!
  private final PyTorchRunJobParameters pyTorchParameters;

  public PyTorchServiceSpec(PyTorchRunJobParameters parameters,
      ClientContext clientContext, FileSystemOperations fsOperations,
      PyTorchLaunchCommandFactory launchCommandFactory, Localizer localizer) {
    super(parameters, clientContext, fsOperations, launchCommandFactory,
        localizer);
    this.pyTorchParameters = parameters;
  }

  @Override
  public ServiceWrapper create() throws IOException {
    LOG.info("Creating PyTorch service spec");
    ServiceWrapper serviceWrapper = createServiceSpecWrapper();

    if (parameters.getNumWorkers() > 0) {
      addWorkerComponents(serviceWrapper, Framework.PYTORCH);
    }

    // After all components added, handle quicklinks
    handleQuicklinks(serviceWrapper.getService());

    return serviceWrapper;
  }

}
