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

package org.apache.hadoop.security.token.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression tests for ZooKeeper operations outside the API write lock. */
@Timeout(90)
public class TestZKDelegationTokenMutations {
  private enum Operation { RENEW, CANCEL, NEW_KEY, RETIRE_KEY, DELETE_KEY }

  private static class Manager extends
      ZKDelegationTokenSecretManager<DelegationTokenIdentifier> {
    private volatile Operation blocked;
    private volatile CountDownLatch entered;
    private volatile CountDownLatch release;
    private volatile boolean fail;
    private int expiredKeyId;

    Manager(String address) {
      this(address, true);
    }

    Manager(String address, boolean watch) {
      super(configuration(address, watch));
    }

    private static Configuration configuration(String address, boolean watch) {
      Configuration conf = new Configuration(false);
      conf.set(ZK_DTSM_ZK_CONNECTION_STRING, address);
      conf.set(ZK_DTSM_ZK_AUTH_TYPE, "none");
      conf.setBoolean(ZK_DTSM_TOKEN_WATCHER_ENABLED, watch);
      return conf;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(new Text("TEST"));
    }

    @Override
    protected void rollMasterKey() {
      // Tests invoke rotation explicitly, without racing background maintenance.
    }

    void rotate() throws IOException {
      super.rollMasterKey();
    }

    void pause(Operation operation) throws IOException {
      if (blocked != operation) {
        return;
      }
      if (fail) {
        throw new IOException("Injected storage failure");
      }
      entered.countDown();
      try {
        if (!release.await(30, TimeUnit.SECONDS)) {
          throw new IOException("Test did not release " + operation);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    void block(Operation operation) {
      entered = new CountDownLatch(1);
      release = new CountDownLatch(1);
      blocked = operation;
    }

    @Override
    protected void addOrUpdateToken(DelegationTokenIdentifier id,
        DelegationTokenInformation info, boolean update) throws Exception {
      if (update) {
        pause(Operation.RENEW);
      }
      super.addOrUpdateToken(id, info, update);
    }

    @Override
    protected void removeStoredToken(DelegationTokenIdentifier id, boolean check)
        throws IOException {
      pause(Operation.CANCEL);
      super.removeStoredToken(id, check);
    }

    @Override
    protected void storeDelegationKey(DelegationKey key) throws IOException {
      pause(Operation.NEW_KEY);
      super.storeDelegationKey(key);
    }

    @Override
    protected void updateDelegationKey(DelegationKey key) throws IOException {
      pause(Operation.RETIRE_KEY);
      super.updateDelegationKey(key);
    }

    @Override
    protected void removeStoredMasterKey(DelegationKey key) {
      try {
        pause(Operation.DELETE_KEY);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      super.removeStoredMasterKey(key);
    }

    Token<DelegationTokenIdentifier> issue() {
      return new Token<>(new DelegationTokenIdentifier(new Text("TEST"),
          new Text("owner"), new Text("renewer"), new Text()), this);
    }

    void verify(Token<DelegationTokenIdentifier> token) throws IOException {
      verifyToken(id(token), token.getPassword());
    }

    void addExpiredKey() throws IOException {
      DelegationKey key = new DelegationKey(incrementCurrentKeyId(), 1L, generateSecret());
      storeDelegationKey(key);
      allKeys.put(key.getKeyId(), key);
      expiredKeyId = key.getKeyId();
    }
  }

  private static DelegationTokenIdentifier id(Token<DelegationTokenIdentifier> token)
      throws IOException {
    DelegationTokenIdentifier id = new DelegationTokenIdentifier(new Text("TEST"));
    try (DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(token.getIdentifier()))) {
      id.readFields(in);
    }
    return id;
  }

  private void verifyDuring(Operation operation) throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      ExecutorService workers = Executors.newFixedThreadPool(3);
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> existing = manager.issue();
        Token<DelegationTokenIdentifier> changed = manager.issue();
        manager.verify(existing);
        manager.verify(changed);
        if (operation == Operation.DELETE_KEY) {
          manager.addExpiredKey();
        }
        manager.block(operation);
        Future<?> mutation = workers.submit(() -> {
          if (operation == Operation.RENEW) {
            manager.renewToken(changed, "renewer");
          } else if (operation == Operation.CANCEL) {
            manager.cancelToken(changed, "owner");
          } else {
            manager.rotate();
          }
          return null;
        });
        assertTrue(manager.entered.await(10, TimeUnit.SECONDS));
        Future<?> verification = workers.submit(() -> {
          manager.verify(existing);
          return null;
        });
        verification.get(2, TimeUnit.SECONDS);
        assertFalse(mutation.isDone());
        if (operation == Operation.NEW_KEY || operation == Operation.RETIRE_KEY) {
          Token<DelegationTokenIdentifier> issued = workers.submit(manager::issue)
              .get(2, TimeUnit.SECONDS);
          assertEquals(id(existing).getMasterKeyId(), id(issued).getMasterKeyId(),
              "A key must not be published before persistence completes");
          manager.verify(issued);
        }
        manager.release.countDown();
        mutation.get(15, TimeUnit.SECONDS);
        manager.verify(existing);
        if (operation == Operation.DELETE_KEY) {
          assertNull(manager.getDelegationKey(manager.expiredKeyId));
        }
        if (operation == Operation.CANCEL) {
          assertNull(manager.getTokenInfoFromZK(id(changed)));
        } else if (operation == Operation.RENEW) {
          manager.verify(changed);
          assertNotNull(manager.getTokenInfoFromZK(id(changed)));
        } else {
          assertNotEquals(id(existing).getMasterKeyId(),
              id(manager.issue()).getMasterKeyId());
        }
      } finally {
        if (manager.release != null) {
          manager.release.countDown();
        }
        workers.shutdownNow();
        assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        manager.stopThreads();
      }
    }
  }

  @Test
  public void testVerifyDuringRenewal() throws Exception {
    verifyDuring(Operation.RENEW);
  }

  @Test
  public void testVerifyDuringCancellation() throws Exception {
    verifyDuring(Operation.CANCEL);
  }

  @Test
  public void testVerifyDuringNewKeyPersistence() throws Exception {
    verifyDuring(Operation.NEW_KEY);
  }

  @Test
  public void testVerifyDuringRetiringKeyPersistence() throws Exception {
    verifyDuring(Operation.RETIRE_KEY);
  }

  @Test
  public void testVerifyDuringExpiredKeyDeletion() throws Exception {
    verifyDuring(Operation.DELETE_KEY);
  }

  @Test
  public void testPeerCancellationWinsOverPausedRenewal() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager first = new Manager(server.getConnectString());
      Manager peer = new Manager(server.getConnectString());
      ExecutorService worker = Executors.newSingleThreadExecutor();
      try {
        first.startThreads();
        peer.startThreads();
        Token<DelegationTokenIdentifier> token = first.issue();
        first.verify(token);
        first.block(Operation.RENEW);
        Future<?> renewal = worker.submit(() -> {
          assertThrows(InvalidToken.class, () -> first.renewToken(token, "renewer"));
        });
        assertTrue(first.entered.await(10, TimeUnit.SECONDS));
        peer.cancelToken(token, "owner");
        first.release.countDown();
        renewal.get(10, TimeUnit.SECONDS);
        assertNull(first.getTokenInfoFromZK(id(token)),
            "Renewal must never recreate a cancelled token");
        assertNull(first.getTokenInfoFromMemory(id(token)));
      } finally {
        if (first.release != null) {
          first.release.countDown();
        }
        worker.shutdownNow();
        assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        first.stopThreads();
        peer.stopThreads();
      }
    }
  }

  @Test
  public void testOlderRenewalCannotShortenPeerRenewal() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager first = new Manager(server.getConnectString());
      Manager peer = new Manager(server.getConnectString());
      try {
        first.startThreads();
        peer.startThreads();
        Token<DelegationTokenIdentifier> token = first.issue();
        DelegationTokenIdentifier id = id(token);
        long later = first.getTokenInfoFromZK(id).getRenewDate() + 1000;
        peer.updateToken(id, new AbstractDelegationTokenSecretManager
            .DelegationTokenInformation(later, token.getPassword()));
        first.updateToken(id, new AbstractDelegationTokenSecretManager
            .DelegationTokenInformation(later - 500, token.getPassword()));
        assertEquals(later, first.getTokenInfoFromZK(id).getRenewDate());
      } finally {
        first.stopThreads();
        peer.stopThreads();
      }
    }
  }

  @Test
  public void testFailedKeyPersistenceKeepsSigningKey() throws Exception {
    for (Operation operation : new Operation[]{Operation.NEW_KEY, Operation.RETIRE_KEY}) {
      try (TestingServer server = new TestingServer()) {
        Manager manager = new Manager(server.getConnectString());
        try {
          manager.startThreads();
          Token<DelegationTokenIdentifier> before = manager.issue();
          manager.block(operation);
          manager.fail = true;
          assertThrows(IOException.class, manager::rotate);
          Token<DelegationTokenIdentifier> after = manager.issue();
          assertEquals(id(before).getMasterKeyId(), id(after).getMasterKeyId());
          manager.verify(before);
          manager.verify(after);
        } finally {
          manager.stopThreads();
        }
      }
    }
  }
  @Test
  public void testLocalCancellationWaitsForRenewal() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      ExecutorService workers = Executors.newFixedThreadPool(2);
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> token = manager.issue();
        manager.verify(token);
        manager.block(Operation.RENEW);
        Future<?> renewal = workers.submit(() -> manager.renewToken(token, "renewer"));
        assertTrue(manager.entered.await(10, TimeUnit.SECONDS));
        CountDownLatch cancelling = new CountDownLatch(1);
        Future<?> cancellation = workers.submit(() -> {
          cancelling.countDown();
          return manager.cancelToken(token, "owner");
        });
        assertTrue(cancelling.await(10, TimeUnit.SECONDS));
        assertThrows(java.util.concurrent.TimeoutException.class,
            () -> cancellation.get(200, TimeUnit.MILLISECONDS));
        manager.release.countDown();
        renewal.get(10, TimeUnit.SECONDS);
        cancellation.get(10, TimeUnit.SECONDS);
        assertNull(manager.getTokenInfoFromZK(id(token)));
      } finally {
        if (manager.release != null) {
          manager.release.countDown();
        }
        workers.shutdownNow();
        assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        manager.stopThreads();
      }
    }
  }

  @Test
  public void testFailedRenewalDoesNotPublish() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> token = manager.issue();
        manager.verify(token);
        DelegationTokenIdentifier id = id(token);
        long previous = manager.getTokenInfoFromZK(id).getRenewDate();
        manager.block(Operation.RENEW);
        manager.fail = true;
        assertThrows(IOException.class, () -> manager.renewToken(token, "renewer"));
        assertEquals(previous, manager.getTokenInfoFromZK(id).getRenewDate());
        assertEquals(previous, manager.getTokenInfoFromMemory(id).getRenewDate());
      } finally {
        manager.stopThreads();
      }
    }
  }

  @Test
  public void testInterruptedRenewalDoesNotPublish() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      ExecutorService worker = Executors.newSingleThreadExecutor();
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> token = manager.issue();
        manager.verify(token);
        long previous = manager.getTokenInfoFromZK(id(token)).getRenewDate();
        manager.block(Operation.RENEW);
        CountDownLatch finished = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean interrupted =
            new java.util.concurrent.atomic.AtomicBoolean();
        Future<?> renewal = worker.submit(() -> {
          try {
            assertThrows(IOException.class, () -> manager.renewToken(token, "renewer"));
            interrupted.set(Thread.currentThread().isInterrupted());
          } finally {
            finished.countDown();
          }
        });
        assertTrue(manager.entered.await(10, TimeUnit.SECONDS));
        renewal.cancel(true);
        assertTrue(finished.await(10, TimeUnit.SECONDS));
        assertTrue(interrupted.get());
        assertEquals(previous, manager.getTokenInfoFromZK(id(token)).getRenewDate());
        assertEquals(previous, manager.getTokenInfoFromMemory(id(token)).getRenewDate());
      } finally {
        if (manager.release != null) {
          manager.release.countDown();
        }
        worker.shutdownNow();
        assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        manager.stopThreads();
      }
    }
  }

  @Test
  public void testRenewalDoesNotReinsertRemovedCacheEntry() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString(), false);
      ExecutorService worker = Executors.newSingleThreadExecutor();
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> token = manager.issue();
        manager.verify(token);
        manager.block(Operation.RENEW);
        Future<?> renewal = worker.submit(() -> manager.renewToken(token, "renewer"));
        assertTrue(manager.entered.await(10, TimeUnit.SECONDS));
        // Simulate cache invalidation while renewal is pending. The durable
        // token is retained so persistence succeeds, testing publication alone.
        manager.currentTokens.remove(id(token));
        manager.release.countDown();
        renewal.get(10, TimeUnit.SECONDS);
        assertNotNull(manager.getTokenInfoFromZK(id(token)));
        assertNull(manager.getTokenInfoFromMemory(id(token)));
      } finally {
        if (manager.release != null) {
          manager.release.countDown();
        }
        worker.shutdownNow();
        assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        manager.stopThreads();
      }
    }
  }

}
