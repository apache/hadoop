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
import org.apache.hadoop.yarn.api.CsiAdaptorPlugin;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.util.csi.CsiConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * NM manages csi-adaptors as a single NM AUX service, this service
 * manages a set of rpc services and each of them serves one particular
 * csi-driver. It loads all available drivers from configuration, and
 * find a csi-driver-adaptor implementation class for each of them. At last
 * it brings up all of them as a composite service.
 */
public class CsiAdaptorServices extends AuxiliaryService {

  private static final Logger LOG =
      LoggerFactory.getLogger(CsiAdaptorServices.class);

  private List<CsiAdaptorProtocolService> serviceList;
  protected CsiAdaptorServices() {
    super(CsiAdaptorServices.class.getName());
    serviceList = new ArrayList<>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // load configuration and init adaptors
    String[] names = CsiConfigUtils.getCsiDriverNames(conf);
    if (names != null && names.length > 0) {
      for (String driverName : names) {
        LOG.info("Adding csi-driver-adaptor for csi-driver {}", driverName);
        CsiAdaptorPlugin serviceImpl = CsiAdaptorFactory
            .getAdaptor(driverName, conf);
        serviceImpl.init(driverName, conf);
        CsiAdaptorProtocolService service =
            new CsiAdaptorProtocolService(serviceImpl);
        serviceList.add(service);
        service.serviceInit(conf);
      }
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (serviceList != null && serviceList.size() > 0) {
      for (CsiAdaptorProtocolService service : serviceList) {
        try {
          service.serviceStop();
        } catch (Exception e) {
          LOG.warn("Unable to stop service " + service.getName(), e);
        }
      }
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    if (serviceList != null && serviceList.size() > 0) {
      for (CsiAdaptorProtocolService service : serviceList) {
        service.serviceStart();
      }
    }
  }

  @Override
  public void initializeApplication(
      ApplicationInitializationContext initAppContext) {
    // do nothing
  }

  @Override
  public void stopApplication(
      ApplicationTerminationContext stopAppContext) {
    // do nothing
  }

  @Override
  public ByteBuffer getMetaData() {
    return ByteBuffer.allocate(0);
  }
}
