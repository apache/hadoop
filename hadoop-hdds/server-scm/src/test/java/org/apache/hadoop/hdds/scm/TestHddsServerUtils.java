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
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.test.PathUtils;

import org.apache.commons.io.FileUtils;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
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

  /**
   * Test {@link ServerUtils#getScmDbDir}.
   */
  @Test
  public void testGetScmDbDir() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File dbDir = new File(testDir, "scmDbDir");
    final File metaDir = new File(testDir, "metaDir");   // should be ignored.
    final Configuration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(dbDir, ServerUtils.getScmDbDir(conf));
      assertTrue(dbDir.exists());          // should have been created.
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  /**
   * Test {@link ServerUtils#getScmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_SCM_DB_DIRS is undefined.
   */
  @Test
  public void testGetScmDbDirWithFallback() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    final Configuration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());
    try {
      assertEquals(metaDir, ServerUtils.getScmDbDir(conf));
      assertTrue(metaDir.exists());        // should have been created.
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoScmDbDirConfigured() {
    thrown.expect(IllegalArgumentException.class);
    ServerUtils.getScmDbDir(new OzoneConfiguration());
  }

  @Test
  public void testGetStaleNodeInterval() {
    final Configuration conf = new OzoneConfiguration();

    // Reset OZONE_SCM_STALENODE_INTERVAL to 300s that
    // larger than max limit value.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 300, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100);
    // the max limit value will be returned
    assertEquals(100000, HddsServerUtil.getStaleNodeInterval(conf));

    // Reset OZONE_SCM_STALENODE_INTERVAL to 10ms that
    // smaller than min limit value.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 10,
        TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100);
    // the min limit value will be returned
    assertEquals(90000, HddsServerUtil.getStaleNodeInterval(conf));
  }
}
