/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.client.ipc;

import com.google.common.base.Preconditions;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.api.types.PingInformation;
import org.apache.slider.api.SliderApplicationApi;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * Implementation of the Slider RESTy Application API over IPC.
 * <p>
 * Operations are executed via the {@link SliderClusterOperations}
 * instance passed in; raised exceptions may be converted into ones
 * consistent with the REST API.
 */
public class SliderApplicationIpcClient implements SliderApplicationApi {

  private static final Logger log =
      LoggerFactory.getLogger(SliderApplicationIpcClient.class);

  private final SliderClusterOperations operations;

  public SliderApplicationIpcClient(SliderClusterOperations operations) {
    Preconditions.checkArgument(operations != null, "null operations");
    this.operations = operations;
  }

  /**
   * Convert received (And potentially unmarshalled) local/remote
   * exceptions into the equivalents in the REST API.
   * Best effort. 
   * <p>
   * If there is no translation, the original exception is returned.
   * <p>
   * If a new exception was created, it will have the message of the 
   * string value of the original exception, and that original
   * exception will be the nested cause of this one
   * @param exception IOException to convert
   * @return an exception to throw
   */
  private IOException convert(IOException exception) {
    IOException result = exception;
    if (exception instanceof NoSuchNodeException) {
      result = new FileNotFoundException(exception.toString());
      result.initCause(exception);
    } else {
      // TODO: remap any other exceptions
    }
    return result;
  }
  
  public SliderApplicationIpcClient(SliderClusterProtocol proxy) {
    this(new SliderClusterOperations(proxy));
  }

  @Override
  public AggregateConf getDesiredModel() throws IOException {
    try {
      return operations.getModelDesired();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ConfTreeOperations getDesiredAppconf() throws IOException {
    try {
      return operations.getModelDesiredAppconf();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ConfTreeOperations getDesiredResources() throws IOException {
    try {
      return operations.getModelDesiredResources();
    } catch (IOException e) {
      throw convert(e);
    }
  }


  @Override
  public void putDesiredResources(ConfTree updated) throws IOException {
    try {
      operations.flex(updated);
    } catch (IOException e) {
      throw convert(e);
    }
  }

  
  @Override
  public AggregateConf getResolvedModel() throws IOException {
    try {
      return operations.getModelResolved();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ConfTreeOperations getResolvedAppconf() throws IOException {
    try {
      return operations.getModelResolvedAppconf();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ConfTreeOperations getResolvedResources() throws IOException {
    try {
      return operations.getModelResolvedResources();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ConfTreeOperations getLiveResources() throws IOException {
    try {
      return operations.getLiveResources();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public Map<String, ContainerInformation> enumContainers() throws IOException {
    try {
      return operations.enumContainers();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ContainerInformation getContainer(String containerId) throws
      IOException {
    try {
      return operations.getContainer(containerId);
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public Map<String, ComponentInformation> enumComponents() throws IOException {
    try {
      return operations.enumComponents();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ComponentInformation getComponent(String componentName) throws IOException {
    try {
      return operations.getComponent(componentName);
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public NodeInformationList getLiveNodes() throws IOException {
    try {
      return operations.getLiveNodes();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public NodeInformation getLiveNode(String hostname) throws IOException {
    try {
      return operations.getLiveNode(hostname);
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public PingInformation ping(String text) throws IOException {
    return null;
  }

  @Override
  public void stop(String text) throws IOException {
    try {
      operations.stop(text);
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public ApplicationLivenessInformation getApplicationLiveness() throws
      IOException {
    try {
      return operations.getApplicationLiveness();
    } catch (IOException e) {
      throw convert(e);
    }
  }

  @Override
  public String toString() {
    return "IPC implementation of SliderApplicationApi bonded to " + operations;
  }
}
