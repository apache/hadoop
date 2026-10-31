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
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.test.TestingServer;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests creation ordering while external storage is slow. */
@Timeout(60)
public class TestZKDelegationTokenCreation {
  private static class Manager extends
      ZKDelegationTokenSecretManager<DelegationTokenIdentifier> {
    private volatile CountDownLatch entered;
    private volatile CountDownLatch release;
    private volatile boolean blockAllocation;
    private volatile long delayMillis;
    private volatile boolean failStorage;

    Manager(String connectString) {
      super(configuration(connectString));
    }

    private static Configuration configuration(String connectString) {
      Configuration conf = new Configuration(false);
      conf.set(ZK_DTSM_ZK_CONNECTION_STRING, connectString);
      conf.set(ZK_DTSM_ZK_AUTH_TYPE, "none");
      conf.setInt(ZK_DTSM_TOKEN_SEQNUM_BATCH_SIZE, 3);
      return conf;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier(new Text("TEST"));
    }

    @Override
    protected void rollMasterKey() {
      // Keep maintenance out of the creation-only concurrency experiment.
    }

    void rotate() throws IOException {
      super.rollMasterKey();
    }

    byte[] sign(DelegationTokenIdentifier id) {
      return createPassword(id.getBytes(), getDelegationKey(id.getMasterKeyId()).getKey());
    }

    private void awaitRelease() {
      entered.countDown();
      try {
        if (!release.await(20, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Test did not release creation");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    @Override
    protected int incrementDelegationTokenSeqNum() {
      if (blockAllocation && entered != null) {
        awaitRelease();
      }
      return super.incrementDelegationTokenSeqNum();
    }

    @Override
    protected void addOrUpdateToken(DelegationTokenIdentifier identifier,
        DelegationTokenInformation info, boolean update) throws Exception {
      if (!update) {
        if (failStorage) {
          throw new IOException("Injected create failure");
        }
        if (!blockAllocation && entered != null) {
          awaitRelease();
        }
        if (delayMillis > 0) {
          Thread.sleep(delayMillis);
        }
      }
      super.addOrUpdateToken(identifier, info, update);
    }

    Token<DelegationTokenIdentifier> issue() {
      return new Token<>(new DelegationTokenIdentifier(
          new Text("TEST"), new Text("owner"), new Text("renewer"),
          new Text()), this);
    }

    void verify(Token<DelegationTokenIdentifier> token) throws IOException {
      verifyToken(identifier(token), token.getPassword());
    }
  }

  private static DelegationTokenIdentifier identifier(
      Token<DelegationTokenIdentifier> token) throws IOException {
    DelegationTokenIdentifier identifier =
        new DelegationTokenIdentifier(new Text("TEST"));
    try (DataInputStream input = new DataInputStream(
        new ByteArrayInputStream(token.getIdentifier()))) {
      identifier.readFields(input);
    }
    return identifier;
  }

  @Test
  public void testVerificationDuringPersistence() throws Exception {
    verifyWhileCreationBlocked(false);
  }

  @Test
  public void testVerificationDuringAllocation() throws Exception {
    verifyWhileCreationBlocked(true);
  }

  private void verifyWhileCreationBlocked(boolean allocation) throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      ExecutorService workers = Executors.newFixedThreadPool(2);
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> existing = manager.issue();
        manager.verify(existing); // Warm the cache before blocking creation.
        manager.blockAllocation = allocation;
        manager.entered = new CountDownLatch(1);
        manager.release = new CountDownLatch(1);
        Future<Token<DelegationTokenIdentifier>> creation =
            workers.submit(manager::issue);
        assertTrue(manager.entered.await(10, TimeUnit.SECONDS));
        assertFalse(creation.isDone(), "Issuance must wait for storage/allocation");
        Future<?> verification = workers.submit(() -> {
          manager.verify(existing);
          return null;
        });
        verification.get(2, TimeUnit.SECONDS);
        assertFalse(creation.isDone(), "Verification must not release creation");
        manager.release.countDown();
        Token<DelegationTokenIdentifier> created = creation.get(10, TimeUnit.SECONDS);
        assertNotNull(manager.getTokenInfoFromZK(identifier(created)));
        manager.verify(created);
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
  public void testConcurrentSequenceRanges() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager first = new Manager(server.getConnectString());
      Manager second = new Manager(server.getConnectString());
      ExecutorService workers = Executors.newFixedThreadPool(8);
      try {
        first.startThreads();
        second.startThreads();
        Set<Future<Token<DelegationTokenIdentifier>>> creations = new HashSet<>();
        for (int i = 0; i < 120; i++) {
          Manager manager = i % 2 == 0 ? first : second;
          creations.add(workers.submit(manager::issue));
        }
        Set<Integer> sequences = new HashSet<>();
        for (Future<Token<DelegationTokenIdentifier>> creation : creations) {
          Token<DelegationTokenIdentifier> token = creation.get(20, TimeUnit.SECONDS);
          assertTrue(sequences.add(identifier(token).getSequenceNumber()));
          assertNotNull(first.getTokenInfoFromZK(identifier(token)));
          first.verify(token);
          second.verify(token);
        }
        assertEquals(120, sequences.size());
      } finally {
        workers.shutdownNow();
        assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        first.stopThreads();
        second.stopThreads();
      }
    }
  }

  @Test
  public void testFailedPersistenceDoesNotPublishToken() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      try {
        manager.startThreads();
        manager.failStorage = true;
        DelegationTokenIdentifier id = new DelegationTokenIdentifier(
            new Text("TEST"), new Text("owner"), new Text("renewer"), new Text());
        assertThrows(RuntimeException.class, () -> new Token<>(id, manager));
        assertNull(manager.getTokenInfoFromMemory(id));
        assertNull(manager.getTokenInfoFromZK(id));
      } finally {
        manager.stopThreads();
      }
    }
  }

  @Test
  public void testInterruptedRefillDoesNotIssueUnreservedNumber() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      try {
        manager.startThreads();
        for (int i = 0; i < 3; i++) {
          manager.incrementDelegationTokenSeqNum();
        }
        Field field = ZKDelegationTokenSecretManager.class
            .getDeclaredField("delTokSeqCounter");
        field.setAccessible(true);
        SharedCount original = (SharedCount) field.get(manager);
        SharedCount counter = spy(original);
        doThrow(new InterruptedException("Injected interruption"))
            .when(counter).trySetCount(any(), anyInt());
        field.set(manager, counter);
        try {
          assertThrows(IllegalStateException.class,
              manager::incrementDelegationTokenSeqNum);
          assertTrue(Thread.currentThread().isInterrupted());
        } finally {
          Thread.interrupted();
          field.set(manager, original);
        }
      } finally {
        manager.stopThreads();
      }
    }
  }

  @Test
  public void testRotationDuringPersistenceUsesPreparedKey() throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      ExecutorService worker = Executors.newSingleThreadExecutor();
      try {
        manager.startThreads();
        manager.entered = new CountDownLatch(1);
        manager.release = new CountDownLatch(1);
        Future<Token<DelegationTokenIdentifier>> creation = worker.submit(manager::issue);
        assertTrue(manager.entered.await(10, TimeUnit.SECONDS));
        // Invoke the actual implementation rather than the maintenance override.
        manager.rotate();
        manager.release.countDown();
        Token<DelegationTokenIdentifier> token = creation.get(10, TimeUnit.SECONDS);
        DelegationTokenIdentifier id = identifier(token);
        assertArrayEquals(token.getPassword(), manager.sign(id));
        manager.verify(token);
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

  /** Opt-in experiment; use the same test against the parent and fixed code. */
  @Test
  @Timeout(300)
  @EnabledIfSystemProperty(named = "token.creation.benchmark", matches = "true")
  public void testVerificationThroughputWithSlowCreation() throws Exception {
    String[] delays = System.getProperty("token.creation.benchmark.delays.ms",
        "0,1,2,5,10,25").split(",");
    int repetitions = Integer.getInteger("token.creation.benchmark.repetitions", 3);
    for (int repetition = 1; repetition <= repetitions; repetition++) {
      for (String delay : delays) {
        measureThroughput(Long.parseLong(delay.trim()), repetition);
      }
    }
  }

  private void measureThroughput(long delayMillis, int repetition) throws Exception {
    try (TestingServer server = new TestingServer()) {
      Manager manager = new Manager(server.getConnectString());
      ExecutorService workers = Executors.newFixedThreadPool(9);
      AtomicBoolean running = new AtomicBoolean(true);
      AtomicBoolean measuring = new AtomicBoolean(false);
      AtomicLong verifications = new AtomicLong();
      AtomicLong creations = new AtomicLong();
      Set<Future<?>> tasks = new HashSet<>();
      try {
        manager.startThreads();
        Token<DelegationTokenIdentifier> existing = manager.issue();
        manager.verify(existing);
        manager.delayMillis = delayMillis;
        tasks.add(workers.submit(() -> {
          while (running.get()) {
            manager.issue();
            if (measuring.get()) {
              creations.incrementAndGet();
            }
          }
        }));
        for (int i = 0; i < 8; i++) {
          tasks.add(workers.submit(() -> {
            while (running.get()) {
              manager.verify(existing);
              if (measuring.get()) {
                verifications.incrementAndGet();
              }
            }
            return null;
          }));
        }
        Thread.sleep(1000); // Warm up before measuring each scenario.
        long start = System.nanoTime();
        measuring.set(true);
        Thread.sleep(3000);
        measuring.set(false);
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        running.set(false);
        for (Future<?> task : tasks) {
          task.get(15, TimeUnit.SECONDS);
        }
        System.out.printf("TOKEN_CREATION_BENCHMARK delayMillis=%d repetition=%d "
            + "seconds=%.3f verifications=%d verifyPerSecond=%.1f "
            + "creations=%d createPerSecond=%.1f%n", delayMillis, repetition,
            seconds, verifications.get(), verifications.get() / seconds,
            creations.get(), creations.get() / seconds);
        assertTrue(creations.get() > 0);
        assertTrue(verifications.get() > 0);
      } finally {
        running.set(false);
        workers.shutdownNow();
        assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        manager.stopThreads();
      }
    }
  }
}
