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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.net.Peer;
import org.junit.Test;

public class TestPeerCache {
  static final Log LOG = LogFactory.getLog(TestPeerCache.class);

  private static final int CAPACITY = 3;
  private static final int EXPIRY_PERIOD = 20;
  private static PeerCache cache =
      PeerCache.getInstance(CAPACITY, EXPIRY_PERIOD);

  private static class FakePeer implements Peer {
    private boolean closed = false;

    private DatanodeID dnId;

    public FakePeer(DatanodeID dnId) {
      this.dnId = dnId;
    }

    @Override
    public ReadableByteChannel getInputStreamChannel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setReadTimeout(int timeoutMs) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getReceiveBufferSize() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getTcpNoDelay() throws IOException {
      return false;
    }

    @Override
    public void setWriteTimeout(int timeoutMs) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
      return closed;
    }
  
    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public String getRemoteAddressString() {
      return dnId.getInfoAddr();
    }

    @Override
    public String getLocalAddressString() {
      return "127.0.0.1:123";
    }

    @Override
    public InputStream getInputStream() throws IOException {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public OutputStream getOutputStream() throws IOException {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public boolean isLocal() {
      return true;
    }
  
    @Override
    public String toString() {
      return "FakePeer(dnId=" + dnId + ")";
    }
  }

  @Test
  public void testAddAndRetrieve() throws Exception {
    DatanodeID dnId = new DatanodeID("192.168.0.1",
          "fakehostname", "fake_storage_id",
          100, 101, 102);
    FakePeer peer = new FakePeer(dnId);
    cache.put(dnId, peer);
    assertTrue(!peer.isClosed());
    assertEquals(1, cache.size());
    assertEquals(peer, cache.get(dnId));
    assertEquals(0, cache.size());
    cache.clear();
  }

  @Test
  public void testExpiry() throws Exception {
    DatanodeID dnIds[] = new DatanodeID[CAPACITY];
    FakePeer peers[] = new FakePeer[CAPACITY];
    for (int i = 0; i < CAPACITY; ++i) {
      dnIds[i] = new DatanodeID("192.168.0.1",
          "fakehostname_" + i, "fake_storage_id",
          100, 101, 102);
      peers[i] = new FakePeer(dnIds[i]);
    }
    for (int i = 0; i < CAPACITY; ++i) {
      cache.put(dnIds[i], peers[i]);
    }
    // Check that the peers are cached
    assertEquals(CAPACITY, cache.size());

    // Wait for the peers to expire
    Thread.sleep(EXPIRY_PERIOD * 50);
    assertEquals(0, cache.size());

    // make sure that the peers were closed when they were expired
    for (int i = 0; i < CAPACITY; ++i) {
      assertTrue(peers[i].isClosed());
    }

    // sleep for another second and see if 
    // the daemon thread runs fine on empty cache
    Thread.sleep(EXPIRY_PERIOD * 50);
    cache.clear();
  }

  @Test
  public void testEviction() throws Exception {
    DatanodeID dnIds[] = new DatanodeID[CAPACITY + 1];
    FakePeer peers[] = new FakePeer[CAPACITY + 1];
    for (int i = 0; i < dnIds.length; ++i) {
      dnIds[i] = new DatanodeID("192.168.0.1",
          "fakehostname_" + i, "fake_storage_id_" + i,
          100, 101, 102);
      peers[i] = new FakePeer(dnIds[i]);
    }
    for (int i = 0; i < CAPACITY; ++i) {
      cache.put(dnIds[i], peers[i]);
    }
    // Check that the peers are cached
    assertEquals(CAPACITY, cache.size());

    // Add another entry and check that the first entry was evicted
    cache.put(dnIds[CAPACITY], peers[CAPACITY]);
    assertEquals(CAPACITY, cache.size());
    assertSame(null, cache.get(dnIds[0]));

    // Make sure that the other entries are still there
    for (int i = 1; i < CAPACITY; ++i) {
      Peer peer = cache.get(dnIds[i]);
      assertSame(peers[i], peer);
      assertTrue(!peer.isClosed());
      peer.close();
    }
    assertEquals(1, cache.size());
    cache.clear();
  }

  @Test
  public void testMultiplePeersWithSameDnId() throws Exception {
    DatanodeID dnId = new DatanodeID("192.168.0.1",
          "fakehostname", "fake_storage_id",
          100, 101, 102);
    HashSet<FakePeer> peers = new HashSet<FakePeer>(CAPACITY);
    for (int i = 0; i < CAPACITY; ++i) {
      FakePeer peer = new FakePeer(dnId);
      peers.add(peer);
      cache.put(dnId, peer);
    }
    // Check that all of the peers ended up in the cache
    assertEquals(CAPACITY, cache.size());
    while (!peers.isEmpty()) {
      Peer peer = cache.get(dnId);
      assertTrue(peer != null);
      assertTrue(!peer.isClosed());
      peers.remove(peer);
    }
    assertEquals(0, cache.size());
    cache.clear();
  }
}
