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
package org.apache.hadoop.security;

import javax.security.sasl.Sasl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test class for IngressPortBasedResolver.
 */
public class TestIngressPortBasedResolver {

  /**
   * A simple test to test that for the configured ports, the resolver
   * can return the current SASL properties.
   */
  @Test
  public void testResolver() {
    Configuration conf = new Configuration();
    conf.set("ingress.port.sasl.configured.ports", "444,555,666,777");
    conf.set("ingress.port.sasl.prop.444", "authentication");
    conf.set("ingress.port.sasl.prop.555", "authentication,privacy");
    conf.set("ingress.port.sasl.prop.666", "privacy");

    IngressPortBasedResolver resolver = new IngressPortBasedResolver();
    resolver.setConf(conf);

    // the client address does not matter, give it a null
    assertEquals("auth",
        resolver.getServerProperties(null, 444).get(Sasl.QOP));
    assertEquals("auth,auth-conf",
        resolver.getServerProperties(null, 555).get(Sasl.QOP));
    assertEquals("auth-conf",
        resolver.getServerProperties(null, 666).get(Sasl.QOP));
    assertEquals("auth-conf",
        resolver.getServerProperties(null, 777).get(Sasl.QOP));
    assertEquals("auth",
        resolver.getServerProperties(null, 888).get(Sasl.QOP));
  }
}
