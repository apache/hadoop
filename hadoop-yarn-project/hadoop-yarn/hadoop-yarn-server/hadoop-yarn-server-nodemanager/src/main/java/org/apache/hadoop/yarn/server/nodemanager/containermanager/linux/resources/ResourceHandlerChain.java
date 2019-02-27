/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A helper class to delegate funcationality to a 'chain' of
 * ResourceHandler(s)
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceHandlerChain implements ResourceHandler {
  private final List<ResourceHandler> resourceHandlers;

  public ResourceHandlerChain(List<ResourceHandler> resourceHandlers) {
    this.resourceHandlers = resourceHandlers;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {

    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.bootstrap(configuration);
      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.preStart(container);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.reacquireContainer(containerId);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.postComplete(containerId);

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    List<PrivilegedOperation> allOperations = new
        ArrayList<PrivilegedOperation>();

    for (ResourceHandler resourceHandler : resourceHandlers) {
      List<PrivilegedOperation> handlerOperations =
          resourceHandler.teardown();

      if (handlerOperations != null) {
        allOperations.addAll(handlerOperations);
      }

    }
    return allOperations;
  }

  @VisibleForTesting
  public List<ResourceHandler> getResourceHandlerList() {
    return Collections.unmodifiableList(resourceHandlers);
  }

  @Override
  public String toString() {
    return ResourceHandlerChain.class.getName() + "{" +
        "resourceHandlers=" + resourceHandlers +
        '}';
  }
}
