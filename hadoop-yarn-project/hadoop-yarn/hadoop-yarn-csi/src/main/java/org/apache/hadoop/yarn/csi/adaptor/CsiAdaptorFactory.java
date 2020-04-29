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
package org.apache.hadoop.yarn.csi.adaptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.CsiAdaptorPlugin;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Desired csi-adaptor implementation is configurable, default to
 * CsiAdaptorProtocolService. If user wants to have a different implementation,
 * just to configure a different class for the csi-driver.
 */
public final class CsiAdaptorFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(CsiAdaptorFactory.class);

  private CsiAdaptorFactory() {
    // hide constructor for this factory class.
  }

  /**
   * Load csi-driver-adaptor from configuration. If the configuration is not
   * specified, the default implementation
   * for the adaptor is {@link DefaultCsiAdaptorImpl}. If the configured class
   * is not a valid variation of {@link CsiAdaptorPlugin} or the class cannot
   * be found, this function will throw a RuntimeException.
   * @param driverName
   * @param conf
   * @return CsiAdaptorPlugin
   * @throws YarnException if unable to create the adaptor class.
   * @throws RuntimeException if given class is not found or not
   *   an instance of {@link CsiAdaptorPlugin}
   */
  public static CsiAdaptorPlugin getAdaptor(String driverName,
      Configuration conf) throws YarnException {
    // load configuration
    String configName = YarnConfiguration.NM_CSI_ADAPTOR_PREFIX
        + driverName + YarnConfiguration.NM_CSI_ADAPTOR_CLASS;
    Class<? extends CsiAdaptorPlugin> impl = conf.getClass(configName,
        DefaultCsiAdaptorImpl.class, CsiAdaptorPlugin.class);
    if (impl == null) {
      throw new YarnException("Unable to init csi-adaptor from the"
          + " class specified via " + configName);
    }

    // init the adaptor
    CsiAdaptorPlugin instance = ReflectionUtils.newInstance(impl, conf);
    LOG.info("csi-adaptor initiated, implementation: "
        + impl.getCanonicalName());
    return instance;
  }
}
