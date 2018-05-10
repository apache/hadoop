/**
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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation.OperationType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

/**
 * ResourceHandler implementation for allocating NUMA Resources to each
 * container.
 */
public class NumaResourceHandlerImpl implements ResourceHandler {

  private static final Log LOG = LogFactory
      .getLog(NumaResourceHandlerImpl.class);
  private NumaResourceAllocator numaResourceAllocator;
  private String numaCtlCmd;

  public NumaResourceHandlerImpl(Configuration conf, Context nmContext) {
    LOG.info("NUMA resources allocation is enabled, initializing NUMA resources"
        + " allocator.");
    numaResourceAllocator = new NumaResourceAllocator(nmContext);
    numaCtlCmd = conf.get(YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD,
        YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    try {
      numaResourceAllocator.init(configuration);
    } catch (YarnException e) {
      throw new ResourceHandlerException(e);
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    List<PrivilegedOperation> ret = null;
    NumaResourceAllocation numaAllocation = numaResourceAllocator
        .allocateNumaNodes(container);
    if (numaAllocation != null) {
      ret = new ArrayList<>();
      ArrayList<String> args = new ArrayList<>();
      args.add(numaCtlCmd);
      args.add(
          "--interleave=" + String.join(",", numaAllocation.getMemNodes()));
      args.add(
          "--cpunodebind=" + String.join(",", numaAllocation.getCpuNodes()));
      ret.add(new PrivilegedOperation(OperationType.ADD_NUMA_PARAMS, args));
    }
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    try {
      numaResourceAllocator.recoverNumaResource(containerId);
    } catch (Throwable e) {
      throw new ResourceHandlerException(
          "Failed to recover numa resource for " + containerId, e);
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    numaResourceAllocator.releaseNumaResource(containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
