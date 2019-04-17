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

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link HddsServerUtil}.
 */
public class TestHddsServerUtils {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestHddsServerUtils.class);

  @Rule
  public Timeout timeout = new Timeout(300_000);

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  /**
   * Test getting OZONE_SCM_DATANODE_ADDRESS_KEY with port.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testGetDatanodeAddressWithPort() {
    final String scmHost = "host123:100";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(address.getPort(), Integer.parseInt(scmHost.split(":")[1]));
  }

  /**
   * Test getting OZONE_SCM_DATANODE_ADDRESS_KEY without port.
   */
  @Test
  public void testGetDatanodeAddressWithoutPort() {
    final String scmHost = "host123";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY is undefined, test fallback to
   * OZONE_SCM_CLIENT_ADDRESS_KEY.
   */
  @Test
  public void testDatanodeAddressFallbackToClientNoPort() {
    final String scmHost = "host123";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY is undefined, test fallback to
   * OZONE_SCM_CLIENT_ADDRESS_KEY. Port number defined by
   * OZONE_SCM_CLIENT_ADDRESS_KEY should be ignored.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testDatanodeAddressFallbackToClientWithPort() {
    final String scmHost = "host123:100";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(address.getPort(), OZONE_SCM_DATANODE_PORT_DEFAULT);
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
   * are undefined, test fallback to OZONE_SCM_NAMES.
   */
  @Test
  public void testDatanodeAddressFallbackToScmNamesNoPort() {
    final String scmHost = "host123";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
   * are undefined, test fallback to OZONE_SCM_NAMES. Port number
   * defined by OZONE_SCM_NAMES should be ignored.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testDatanodeAddressFallbackToScmNamesWithPort() {
    final String scmHost = "host123:100";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * getScmAddressForDataNodes should fail when OZONE_SCM_NAMES has
   * multiple addresses.
   */
  @Test
  public void testClientFailsWithMultipleScmNames() {
    final String scmHost = "host123,host456";
    final Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    thrown.expect(IllegalArgumentException.class);
    HddsServerUtil.getScmAddressForDataNodes(conf);
  }

}
