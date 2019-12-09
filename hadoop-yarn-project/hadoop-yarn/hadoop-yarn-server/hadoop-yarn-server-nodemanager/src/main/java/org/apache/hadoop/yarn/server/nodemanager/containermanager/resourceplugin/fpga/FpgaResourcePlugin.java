/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceHandlerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

public class FpgaResourcePlugin implements ResourcePlugin {
  private static final Log LOG = LogFactory.getLog(FpgaResourcePlugin.class);

  private ResourceHandler fpgaResourceHandler = null;

  private AbstractFpgaVendorPlugin vendorPlugin = null;
  private FpgaNodeResourceUpdateHandler fpgaNodeResourceUpdateHandler = null;

  private AbstractFpgaVendorPlugin createFpgaVendorPlugin(Configuration conf) {
    String vendorPluginClass = conf.get(YarnConfiguration.NM_FPGA_VENDOR_PLUGIN,
        YarnConfiguration.DEFAULT_NM_FPGA_VENDOR_PLUGIN);
    LOG.info("Using FPGA vendor plugin: " + vendorPluginClass);
    try {
      Class<?> schedulerClazz = Class.forName(vendorPluginClass);
      if (AbstractFpgaVendorPlugin.class.isAssignableFrom(schedulerClazz)) {
        return (AbstractFpgaVendorPlugin) ReflectionUtils.newInstance(schedulerClazz,
            conf);
      } else {
        throw new YarnRuntimeException("Class: " + vendorPluginClass
            + " not instance of " + AbstractFpgaVendorPlugin.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate FPGA vendor plugin: "
          + vendorPluginClass, e);
    }
  }

  @Override
  public void initialize(Context context) throws YarnException {
    // Get vendor plugin from configuration
    this.vendorPlugin = createFpgaVendorPlugin(context.getConf());
    FpgaDiscoverer.getInstance().setResourceHanderPlugin(vendorPlugin);
    FpgaDiscoverer.getInstance().initialize(context.getConf());
    fpgaNodeResourceUpdateHandler = new FpgaNodeResourceUpdateHandler();
  }

  @Override
  public ResourceHandler createResourceHandler(
      Context nmContext, CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    if (fpgaResourceHandler == null) {
      fpgaResourceHandler = new FpgaResourceHandlerImpl(nmContext,
          cGroupsHandler, privilegedOperationExecutor, vendorPlugin);
    }
    return fpgaResourceHandler;
  }

  @Override
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    return fpgaNodeResourceUpdateHandler;
  }

  @Override
  public void cleanup() throws YarnException {

  }

  @Override
  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return null;
  }

  @Override
  public NMResourceInfo getNMResourceInfo() throws YarnException {
    return null;
  }

  @Override
  public String toString() {
    return FpgaResourcePlugin.class.getName();
  }
}
