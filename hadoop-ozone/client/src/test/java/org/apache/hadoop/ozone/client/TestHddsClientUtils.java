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
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.net.InetSocketAddress;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * This test class verifies the parsing of SCM endpoint config settings. The
 * parsing logic is in
 * {@link org.apache.hadoop.hdds.scm.client.HddsClientUtils}.
 */
public class TestHddsClientUtils {
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
    HddsUtils.getScmAddressForClients(conf);
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
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = HddsUtils.getScmAddressForClients(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(OZONE_SCM_CLIENT_PORT_DEFAULT));

    // Next try a client address with a host name and port. Verify both
    // are used correctly.
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    addr = HddsUtils.getScmAddressForClients(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(100));
  }

  @Test
  public void testgetOmSocketAddress() {
    final Configuration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = OmUtils.getOmAddress(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(OMConfigKeys.OZONE_OM_PORT_DEFAULT));

    // Next try a client address with just a host name and port. Verify the port
    // is ignored and the default OM port is used.
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "1.2.3.4:100");
    addr = OmUtils.getOmAddress(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(100));

    // Assert the we are able to use default configs if no value is specified.
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "");
    addr = OmUtils.getOmAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(OMConfigKeys.OZONE_OM_PORT_DEFAULT));
  }

  @Test
  public void testBlockClientFallbackToClientNoPort() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY is undefined it should
    // fallback to OZONE_SCM_CLIENT_ADDRESS_KEY.
    final String scmHost = "host123";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address = HddsUtils.getScmAddressForBlockClients(
        conf);
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  @SuppressWarnings("StringSplitter")
  public void testBlockClientFallbackToClientWithPort() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY is undefined it should
    // fallback to OZONE_SCM_CLIENT_ADDRESS_KEY.
    //
    // Verify that the OZONE_SCM_CLIENT_ADDRESS_KEY port number is ignored,
    // if present. Instead we should use OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT.
    final String scmHost = "host123:100";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =HddsUtils.getScmAddressForBlockClients(
        conf);
    assertEquals(scmHost.split(":")[0], address.getHostName());
    assertEquals(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  public void testBlockClientFallbackToScmNamesNoPort() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
    // are undefined it should fallback to OZONE_SCM_NAMES.
    final String scmHost = "host456";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address = HddsUtils.getScmAddressForBlockClients(
        conf);
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  @SuppressWarnings("StringSplitter")
  public void testBlockClientFallbackToScmNamesWithPort() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
    // are undefined it should fallback to OZONE_SCM_NAMES.
    //
    // Verify that the OZONE_SCM_NAMES port number is ignored, if present.
    // Instead we should use OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT.
    final String scmHost = "host456:200";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address = HddsUtils.getScmAddressForBlockClients(
        conf);
    assertEquals(scmHost.split(":")[0], address.getHostName());
    assertEquals(OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  public void testClientFallbackToScmNamesNoPort() {
    // When OZONE_SCM_CLIENT_ADDRESS_KEY is undefined, it should fallback
    // to OZONE_SCM_NAMES.
    final String scmHost = "host456";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address = HddsUtils.getScmAddressForClients(conf);
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  @SuppressWarnings("StringSplitter")
  public void testClientFallbackToScmNamesWithPort() {
    // When OZONE_SCM_CLIENT_ADDRESS_KEY is undefined, it should fallback
    // to OZONE_SCM_NAMES.
    //
    // Verify that the OZONE_SCM_NAMES port number is ignored, if present.
    // Instead we should use OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT.
    final String scmHost = "host456:300";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address = HddsUtils.getScmAddressForClients(conf);
    assertEquals(scmHost.split(":")[0], address.getHostName());
    assertEquals(OZONE_SCM_CLIENT_PORT_DEFAULT, address.getPort());
  }

  @Test
  public void testBlockClientFailsWithMultipleScmNames() {
    // When OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
    // are undefined, fail if OZONE_SCM_NAMES has multiple SCMs.
    final String scmHost = "host123,host456";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    thrown.expect(IllegalArgumentException.class);
    HddsUtils.getScmAddressForBlockClients(conf);
  }

  @Test
  public void testClientFailsWithMultipleScmNames() {
    // When OZONE_SCM_CLIENT_ADDRESS_KEY is undefined, fail if OZONE_SCM_NAMES
    // has multiple SCMs.
    final String scmHost = "host123,host456";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    thrown.expect(IllegalArgumentException.class);
    HddsUtils.getScmAddressForClients(conf);
  }
}
