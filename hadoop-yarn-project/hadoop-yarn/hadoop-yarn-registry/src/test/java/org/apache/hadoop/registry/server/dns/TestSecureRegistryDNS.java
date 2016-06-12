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
package org.apache.hadoop.registry.server.dns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryConstants;

/**
 *
 */
public class TestSecureRegistryDNS extends TestRegistryDNS {
  @Override protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(RegistryConstants.KEY_DNSSEC_ENABLED, true);
    conf.set(RegistryConstants.KEY_DNSSEC_PUBLIC_KEY,
        "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
            + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
            + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
            + "l9Ozs5bV");
    conf.set(RegistryConstants.KEY_DNSSEC_PRIVATE_KEY_FILE,
        getClass().getResource("/test.private").getFile());

    return conf;
  }

  @Override protected boolean isSecure() {
    return true;
  }

}
