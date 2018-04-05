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
package org.apache.hadoop.hdds.server;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Common ozone/hdds web methods.
 */
public class TestBaseHttpServer {
  @Test
  public void getBindAddress() throws Exception {
    Configuration conf = new Configuration();
    conf.set("enabled", "false");

    BaseHttpServer baseHttpServer = new BaseHttpServer(conf, "test") {
      @Override
      protected String getHttpAddressKey() {
        return null;
      }

      @Override
      protected String getHttpsAddressKey() {
        return null;
      }

      @Override
      protected String getHttpBindHostKey() {
        return null;
      }

      @Override
      protected String getHttpsBindHostKey() {
        return null;
      }

      @Override
      protected String getBindHostDefault() {
        return null;
      }

      @Override
      protected int getHttpBindPortDefault() {
        return 0;
      }

      @Override
      protected int getHttpsBindPortDefault() {
        return 0;
      }

      @Override
      protected String getKeytabFile() {
        return null;
      }

      @Override
      protected String getSpnegoPrincipal() {
        return null;
      }

      @Override
      protected String getEnabledKey() {
        return "enabled";
      }
    };

    conf.set("addresskey", "0.0.0.0:1234");

    Assert.assertEquals("/0.0.0.0:1234", baseHttpServer
        .getBindAddress("bindhostkey", "addresskey",
            "default", 65).toString());

    conf.set("bindhostkey", "1.2.3.4");

    Assert.assertEquals("/1.2.3.4:1234", baseHttpServer
        .getBindAddress("bindhostkey", "addresskey",
            "default", 65).toString());
  }

}