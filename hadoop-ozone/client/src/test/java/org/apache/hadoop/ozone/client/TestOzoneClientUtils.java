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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ksm.KSMConfigKeys;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.net.InetSocketAddress;

import static org.apache.hadoop.hdsl.HdslUtils.getScmAddressForClients;
import static org.apache.hadoop.ozone.KsmUtils.getKsmAddress;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * This test class verifies the parsing of SCM endpoint config settings.
 * The parsing logic is in {@link OzoneClientUtils}.
 */
public class TestOzoneClientUtils {
  @Rule
  public Timeout timeout = new Timeout(300000);

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  /**
   * Verify client endpoint lookup failure if it is not configured.
   */
  @Test
  public void testMissingScmClientAddress() {
    final Configuration conf = new OzoneConfiguration();
    thrown.expect(IllegalArgumentException.class);
    getScmAddressForClients(conf);
  }

  /**
   * Verify that the client endpoint can be correctly parsed from
   * configuration.
   */
  @Test
  public void testGetScmClientAddress() {
    final Configuration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = getScmAddressForClients(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));

    // Next try a client address with a host name and port. Verify both
    // are used correctly.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    addr = getScmAddressForClients(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(100));
  }

  @Test
  public void testGetKSMAddress() {
    final Configuration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(KSMConfigKeys.OZONE_KSM_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = getKsmAddress(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(KSMConfigKeys.OZONE_KSM_PORT_DEFAULT));

    // Next try a client address with just a host name and port. Verify the port
    // is ignored and the default KSM port is used.
    conf.set(KSMConfigKeys.OZONE_KSM_ADDRESS_KEY, "1.2.3.4:100");
    addr = getKsmAddress(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(100));

    // Assert the we are able to use default configs if no value is specified.
    conf.set(KSMConfigKeys.OZONE_KSM_ADDRESS_KEY, "");
    addr = getKsmAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(KSMConfigKeys.OZONE_KSM_PORT_DEFAULT));
  }
}
