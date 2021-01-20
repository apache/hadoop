/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.RuncContainerExecutorConfig.OCIRuntimeConfig.OCIMount;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides a sample runC container transformation using
 * the RuncContainerTransformerPlugin. This will mount in cluster kerberos
 * configurations to all runC containers.
 */
public class KerberosContainerTransformerPlugin extends AbstractService
    implements RuncContainerTransformerPlugin {

  private static final Log LOG =
      LogFactory.getLog(KerberosContainerTransformerPlugin.class);
  private String kerberosConfigFile;
  private static final String DEFAULT_KRB5_CONFIG_FILE = "/etc/krb5.conf";
  private static final String RUNC_CONTAINER_KRB5_CONFIG_FILE =
      "YARN_CONTAINER_KRB5_CONFIG_FILE";

  public KerberosContainerTransformerPlugin() {
    super(KerberosContainerTransformerPlugin.class.getName());
  }

  public RuncContainerExecutorConfig transform(RuncContainerExecutorConfig
      originalContainerConfig) throws ContainerExecutionException {

    OCIRuntimeConfig runtimeConfig =
        originalContainerConfig.getOciRuntimeConfig();
    List<OCIMount> existingMounts = runtimeConfig.getMounts();

    addRuncMount(existingMounts, kerberosConfigFile,
        DEFAULT_KRB5_CONFIG_FILE, false);

    return originalContainerConfig;
  }

  private void addRuncMount(List<OCIMount> mounts, String srcPath,
      String destPath, boolean isReadWrite) throws ContainerExecutionException {

    ArrayList<String> mountOptions = new ArrayList<>();
    if (isReadWrite) {
      mountOptions.add("rw");
    } else {
      mountOptions.add("ro");
    }
    mountOptions.add("rbind");
    mountOptions.add("rprivate");

    if (sourceExists(srcPath)) {
      mounts.add(new OCIMount(destPath, "bind", srcPath, mountOptions));
      LOG.debug("Adding runC mount " + destPath + " using source " + srcPath);
    } else {
      throw new ContainerExecutionException("The source path does not " +
        "exist, failed to add runC mount " + destPath);
    }
  }

  private boolean sourceExists(String source) {
    return new File(source).exists();
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    // Optional YARN config property
    String configFile = configuration.get(RUNC_CONTAINER_KRB5_CONFIG_FILE);
    if (configFile == null) {
      kerberosConfigFile = DEFAULT_KRB5_CONFIG_FILE;
      LOG.debug("Using the default kerberos config file");
    } else {
      kerberosConfigFile = configFile;
      LOG.debug("Using kerberos config file: " + kerberosConfigFile);
    }
  }

}
