/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.services.security;

import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class KeystoreGenerator extends AbstractSecurityStoreGenerator {


  public KeystoreGenerator(CertificateManager certificateMgr) {
    super(certificateMgr);
  }

  @Override
  public SecurityStore generate(String hostname, String containerId,
                                AggregateConf instanceDefinition,
                                MapOperations compOps, String role)
      throws SliderException, IOException {
    SecurityStore keystore = null;
    String password = getStorePassword(
        instanceDefinition.getAppConf().credentials, compOps, role);
    if (password != null) {
      keystore =
          certificateMgr.generateContainerKeystore(hostname, containerId, role,
                                                   password);
    }
    return keystore;
  }

  @Override
  String getPassword(MapOperations compOps) {
    return compOps.get(
        compOps.get(SliderKeys.COMP_KEYSTORE_PASSWORD_PROPERTY_KEY));
  }

  @Override
  String getAlias(MapOperations compOps) {
    return compOps.getOption(SliderKeys.COMP_KEYSTORE_PASSWORD_ALIAS_KEY,
                             SliderKeys.COMP_KEYSTORE_PASSWORD_ALIAS_DEFAULT);
  }
}
