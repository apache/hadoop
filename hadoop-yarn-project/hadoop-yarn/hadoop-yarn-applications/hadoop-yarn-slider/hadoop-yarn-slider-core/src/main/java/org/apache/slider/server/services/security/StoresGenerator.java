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

import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class StoresGenerator {

  static CertificateManager certMgr = new CertificateManager();
  private static SecurityStoreGenerator[] GENERATORS = {
      new KeystoreGenerator(certMgr), new TruststoreGenerator(certMgr)
  };

  public static SecurityStore[] generateSecurityStores(String hostname,
                                                       String containerId,
                                                       String role,
                                                       AggregateConf instanceDefinition,
                                                       MapOperations compOps)
      throws SliderException, IOException {
    //discover which stores need generation based on the passwords configured
    List<SecurityStore> files = new ArrayList<SecurityStore>();
    for (SecurityStoreGenerator generator : GENERATORS) {
      if (generator.isStoreRequested(compOps)) {
        SecurityStore store = generator.generate(hostname,
                                                 containerId,
                                                 instanceDefinition,
                                                 compOps,
                                                 role);
        if (store != null) {
          files.add(store);
        }
      }
    }

    if (files.isEmpty()) {
      throw new SliderException("Security stores were requested but none were "
                                + "generated. Check the AM logs and ensure "
                                + "passwords are configured for the components "
                                + "requiring the stores.");
    }
    return files.toArray(new SecurityStore[files.size()]);
  }

}
