/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security.authentication.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.Test;

public class TestKerberosUtil {

  @Test
  public void testGetServerPrincipal() throws IOException {
    String service = "TestKerberosUtil";
    String localHostname = KerberosUtil.getLocalHostName();
    String testHost = "FooBar";

    // send null hostname
    assertEquals("When no hostname is sent",
        service + "/" + localHostname.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, null));
    // send empty hostname
    assertEquals("When empty hostname is sent",
        service + "/" + localHostname.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, ""));
    // send 0.0.0.0 hostname
    assertEquals("When 0.0.0.0 hostname is sent",
        service + "/" + localHostname.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, "0.0.0.0"));
    // send uppercase hostname
    assertEquals("When uppercase hostname is sent",
        service + "/" + testHost.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, testHost));
    // send lowercase hostname
    assertEquals("When lowercase hostname is sent",
        service + "/" + testHost.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, testHost.toLowerCase()));
  }
}