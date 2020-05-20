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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressTestHelper.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Status.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.junit.Before;
import org.junit.Test;

public class TestStartupProgress {

  private StartupProgress startupProgress;

  @Before
  public void setUp() {
    startupProgress = new StartupProgress();
  }

  @Test(timeout=10000)
  public void testCounter() {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    Step loadingFsImageInodes = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageInodes);
    incrementCounter(startupProgress, LOADING_FSIMAGE, loadingFsImageInodes,
      100L);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Step loadingFsImageDelegationKeys = new Step(DELEGATION_KEYS);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    incrementCounter(startupProgress, LOADING_FSIMAGE,
      loadingFsImageDelegationKeys, 200L);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.endPhase(LOADING_FSIMAGE);

    startupProgress.beginPhase(LOADING_EDITS);
    Step loadingEditsFile = new Step("file", 1000L);
    startupProgress.beginStep(LOADING_EDITS, loadingEditsFile);
    incrementCounter(startupProgress, LOADING_EDITS, loadingEditsFile, 5000L);

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(100L, view.getCount(LOADING_FSIMAGE, loadingFsImageInodes));
    assertEquals(200L, view.getCount(LOADING_FSIMAGE,
      loadingFsImageDelegationKeys));
    assertEquals(5000L, view.getCount(LOADING_EDITS, loadingEditsFile));
    assertEquals(0L, view.getCount(SAVING_CHECKPOINT,
      new Step(INODES)));

    // Increment a counter again and check that the existing view was not
    // modified, but a new view shows the updated value.
    incrementCounter(startupProgress, LOADING_EDITS, loadingEditsFile, 1000L);
    startupProgress.endStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.endPhase(LOADING_EDITS);

    assertEquals(5000L, view.getCount(LOADING_EDITS, loadingEditsFile));
    view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(6000L, view.getCount(LOADING_EDITS, loadingEditsFile));
  }

  @Test(timeout=10000)
  public void testElapsedTime() throws Exception {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    Step loadingFsImageInodes = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Thread.sleep(50L); // brief sleep to fake elapsed time
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Step loadingFsImageDelegationKeys = new Step(DELEGATION_KEYS);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    Thread.sleep(50L); // brief sleep to fake elapsed time
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.endPhase(LOADING_FSIMAGE);

    startupProgress.beginPhase(LOADING_EDITS);
    Step loadingEditsFile = new Step("file", 1000L);
    startupProgress.beginStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.setTotal(LOADING_EDITS, loadingEditsFile, 10000L);
    incrementCounter(startupProgress, LOADING_EDITS, loadingEditsFile, 5000L);
    Thread.sleep(50L); // brief sleep to fake elapsed time

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertTrue(view.getElapsedTime() > 0);

    assertTrue(view.getElapsedTime(LOADING_FSIMAGE) > 0);
    assertTrue(view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes) > 0);
    assertTrue(view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageDelegationKeys) > 0);

    assertTrue(view.getElapsedTime(LOADING_EDITS) > 0);
    assertTrue(view.getElapsedTime(LOADING_EDITS, loadingEditsFile) > 0);

    assertTrue(view.getElapsedTime(SAVING_CHECKPOINT) == 0);
    assertTrue(view.getElapsedTime(SAVING_CHECKPOINT,
      new Step(INODES)) == 0);

    // Brief sleep, then check that completed phases/steps have the same elapsed
    // time, but running phases/steps have updated elapsed time.
    long totalTime = view.getElapsedTime();
    long loadingFsImageTime = view.getElapsedTime(LOADING_FSIMAGE);
    long loadingFsImageInodesTime = view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes);
    long loadingFsImageDelegationKeysTime = view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes);
    long loadingEditsTime = view.getElapsedTime(LOADING_EDITS);
    long loadingEditsFileTime = view.getElapsedTime(LOADING_EDITS,
      loadingEditsFile);

    Thread.sleep(50L);

    assertTrue(totalTime < view.getElapsedTime());
    assertEquals(loadingFsImageTime, view.getElapsedTime(LOADING_FSIMAGE));
    assertEquals(loadingFsImageInodesTime, view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes));
    assertTrue(loadingEditsTime < view.getElapsedTime(LOADING_EDITS));
    assertTrue(loadingEditsFileTime < view.getElapsedTime(LOADING_EDITS,
      loadingEditsFile));
  }

  @Test(timeout=10000)
  public void testFrozenAfterStartupCompletes() {
    // Do some updates and counter increments.
    startupProgress.beginPhase(LOADING_FSIMAGE);
    startupProgress.setFile(LOADING_FSIMAGE, "file1");
    startupProgress.setSize(LOADING_FSIMAGE, 1000L);
    Step step = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, step);
    startupProgress.setTotal(LOADING_FSIMAGE, step, 10000L);
    incrementCounter(startupProgress, LOADING_FSIMAGE, step, 100L);
    startupProgress.endStep(LOADING_FSIMAGE, step);
    startupProgress.endPhase(LOADING_FSIMAGE);

    StartupProgressView beforePhaseUpdate = startupProgress.createView();

    // LOADING_FSIMAGE phase has been completed, but attempt more updates to it
    Step fsimageStep2 = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, fsimageStep2);
    incrementCounter(startupProgress, LOADING_FSIMAGE, fsimageStep2, 1000000L);
    startupProgress.endStep(LOADING_FSIMAGE, fsimageStep2);

    // Force completion of phases, so that entire startup process is completed.
    for (Phase phase: EnumSet.allOf(Phase.class)) {
      if (startupProgress.getStatus(phase) != Status.COMPLETE) {
        startupProgress.beginPhase(phase);
        startupProgress.endPhase(phase);
      }
    }

    StartupProgressView before = startupProgress.createView();

    // Attempt more updates and counter increments.
    startupProgress.beginPhase(LOADING_FSIMAGE);
    startupProgress.setFile(LOADING_FSIMAGE, "file2");
    startupProgress.setSize(LOADING_FSIMAGE, 2000L);
    startupProgress.beginStep(LOADING_FSIMAGE, step);
    startupProgress.setTotal(LOADING_FSIMAGE, step, 20000L);
    incrementCounter(startupProgress, LOADING_FSIMAGE, step, 100L);
    startupProgress.endStep(LOADING_FSIMAGE, step);
    startupProgress.endPhase(LOADING_FSIMAGE);

    // Also attempt a whole new step that wasn't used last time.
    startupProgress.beginPhase(LOADING_EDITS);
    Step newStep = new Step("file1");
    startupProgress.beginStep(LOADING_EDITS, newStep);
    incrementCounter(startupProgress, LOADING_EDITS, newStep, 100L);
    startupProgress.endStep(LOADING_EDITS, newStep);
    startupProgress.endPhase(LOADING_EDITS);

    StartupProgressView after = startupProgress.createView();

    // Expect that data was frozen after completion of entire startup process, so
    // second set of updates and counter increments should have had no effect.
    assertViewEquals(before, after, LOADING_FSIMAGE, step, fsimageStep2);
    assertEquals(before.getElapsedTime(), after.getElapsedTime());

    // After the phase was completed but before startup was completed,
    // everything should be equal, except for the total elapsed time
    assertViewEquals(beforePhaseUpdate, after, LOADING_FSIMAGE,
        step, fsimageStep2);

    assertFalse(after.getSteps(LOADING_EDITS).iterator().hasNext());
  }

  private void assertViewEquals(StartupProgressView view1,
      StartupProgressView view2, Phase phaseToVerify, Step... stepsToVerify) {
    assertEquals(view1.getCount(phaseToVerify),
        view2.getCount(phaseToVerify));
    assertEquals(view1.getElapsedTime(phaseToVerify),
        view2.getElapsedTime(phaseToVerify));
    assertEquals(view1.getFile(phaseToVerify),
        view2.getFile(phaseToVerify));
    assertEquals(view1.getSize(phaseToVerify),
        view2.getSize(phaseToVerify));
    assertEquals(view1.getTotal(phaseToVerify),
        view2.getTotal(phaseToVerify));
    for (Step step : stepsToVerify) {
      assertEquals(view1.getCount(phaseToVerify, step),
          view2.getCount(phaseToVerify, step));
      assertEquals(view1.getElapsedTime(phaseToVerify, step),
          view2.getElapsedTime(phaseToVerify, step));
      assertEquals(view1.getTotal(phaseToVerify, step),
          view2.getTotal(phaseToVerify, step));
    }
  }

  @Test(timeout=10000)
  public void testInitialState() {
    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(0L, view.getElapsedTime());
    assertEquals(0.0f, view.getPercentComplete(), 0.001f);
    List<Phase> phases = new ArrayList<Phase>();

    for (Phase phase: view.getPhases()) {
      phases.add(phase);
      assertEquals(0L, view.getElapsedTime(phase));
      assertNull(view.getFile(phase));
      assertEquals(0.0f, view.getPercentComplete(phase), 0.001f);
      assertEquals(Long.MIN_VALUE, view.getSize(phase));
      assertEquals(PENDING, view.getStatus(phase));
      assertEquals(0L, view.getTotal(phase));

      for (Step step: view.getSteps(phase)) {
        fail(String.format("unexpected step %s in phase %s at initial state",
          step, phase));
      }
    }

    assertArrayEquals(EnumSet.allOf(Phase.class).toArray(), phases.toArray());
  }

  @Test(timeout=10000)
  public void testPercentComplete() {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    Step loadingFsImageInodes = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageInodes);
    startupProgress.setTotal(LOADING_FSIMAGE, loadingFsImageInodes, 1000L);
    incrementCounter(startupProgress, LOADING_FSIMAGE, loadingFsImageInodes,
      100L);
    Step loadingFsImageDelegationKeys = new Step(DELEGATION_KEYS);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.setTotal(LOADING_FSIMAGE, loadingFsImageDelegationKeys,
      800L);
    incrementCounter(startupProgress, LOADING_FSIMAGE,
      loadingFsImageDelegationKeys, 200L);

    startupProgress.beginPhase(LOADING_EDITS);
    Step loadingEditsFile = new Step("file", 1000L);
    startupProgress.beginStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.setTotal(LOADING_EDITS, loadingEditsFile, 10000L);
    incrementCounter(startupProgress, LOADING_EDITS, loadingEditsFile, 5000L);

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(0.167f, view.getPercentComplete(), 0.001f);
    assertEquals(0.167f, view.getPercentComplete(LOADING_FSIMAGE), 0.001f);
    assertEquals(0.10f, view.getPercentComplete(LOADING_FSIMAGE,
      loadingFsImageInodes), 0.001f);
    assertEquals(0.25f, view.getPercentComplete(LOADING_FSIMAGE,
      loadingFsImageDelegationKeys), 0.001f);
    assertEquals(0.5f, view.getPercentComplete(LOADING_EDITS), 0.001f);
    assertEquals(0.5f, view.getPercentComplete(LOADING_EDITS, loadingEditsFile),
      0.001f);
    assertEquals(0.0f, view.getPercentComplete(SAVING_CHECKPOINT), 0.001f);
    assertEquals(0.0f, view.getPercentComplete(SAVING_CHECKPOINT,
      new Step(INODES)), 0.001f);

    // End steps/phases, and confirm that they jump to 100% completion.
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageInodes);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.endPhase(LOADING_FSIMAGE);
    startupProgress.endStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.endPhase(LOADING_EDITS);

    view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(0.5f, view.getPercentComplete(), 0.001f);
    assertEquals(1.0f, view.getPercentComplete(LOADING_FSIMAGE), 0.001f);
    assertEquals(1.0f, view.getPercentComplete(LOADING_FSIMAGE,
      loadingFsImageInodes), 0.001f);
    assertEquals(1.0f, view.getPercentComplete(LOADING_FSIMAGE,
      loadingFsImageDelegationKeys), 0.001f);
    assertEquals(1.0f, view.getPercentComplete(LOADING_EDITS), 0.001f);
    assertEquals(1.0f, view.getPercentComplete(LOADING_EDITS, loadingEditsFile),
      0.001f);
    assertEquals(0.0f, view.getPercentComplete(SAVING_CHECKPOINT), 0.001f);
    assertEquals(0.0f, view.getPercentComplete(SAVING_CHECKPOINT,
      new Step(INODES)), 0.001f);
  }

  @Test(timeout=10000)
  public void testStatus() {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    startupProgress.endPhase(LOADING_FSIMAGE);
    startupProgress.beginPhase(LOADING_EDITS);
    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(COMPLETE, view.getStatus(LOADING_FSIMAGE));
    assertEquals(RUNNING, view.getStatus(LOADING_EDITS));
    assertEquals(PENDING, view.getStatus(SAVING_CHECKPOINT));
  }

  @Test(timeout=10000)
  public void testStepSequence() {
    // Test that steps are returned in the correct sort order (by file and then
    // sequence number) by starting a few steps in a randomly shuffled order and
    // then asserting that they are returned in the expected order.
    Step[] expectedSteps = new Step[] {
      new Step(INODES, "file1"),
      new Step(DELEGATION_KEYS, "file1"),
      new Step(INODES, "file2"),
      new Step(DELEGATION_KEYS, "file2"),
      new Step(INODES, "file3"),
      new Step(DELEGATION_KEYS, "file3")
    };

    List<Step> shuffledSteps = new ArrayList<Step>(Arrays.asList(expectedSteps));
    Collections.shuffle(shuffledSteps);

    startupProgress.beginPhase(SAVING_CHECKPOINT);
    for (Step step: shuffledSteps) {
      startupProgress.beginStep(SAVING_CHECKPOINT, step);
    }

    List<Step> actualSteps = new ArrayList<Step>(expectedSteps.length);
    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    for (Step step: view.getSteps(SAVING_CHECKPOINT)) {
      actualSteps.add(step);
    }

    assertArrayEquals(expectedSteps, actualSteps.toArray());
  }

  @Test(timeout=10000)
  public void testThreadSafety() throws Exception {
    // Test for thread safety by starting multiple threads that mutate the same
    // StartupProgress instance in various ways.  We expect no internal
    // corruption of data structures and no lost updates on counter increments.
    int numThreads = 100;

    // Data tables used by each thread to determine values to pass to APIs.
    Phase[] phases = { LOADING_FSIMAGE, LOADING_FSIMAGE, LOADING_EDITS,
      LOADING_EDITS };
    Step[] steps = new Step[] { new Step(INODES), new Step(DELEGATION_KEYS),
      new Step(INODES), new Step(DELEGATION_KEYS) };
    String[] files = { "file1", "file1", "file2", "file2" };
    long[] sizes = { 1000L, 1000L, 2000L, 2000L };
    long[] totals = { 10000L, 20000L, 30000L, 40000L };

    ExecutorService exec = Executors.newFixedThreadPool(numThreads);

    try {
      for (int i = 0; i < numThreads; ++i) {
        final Phase phase = phases[i % phases.length];
        final Step step = steps[i % steps.length];
        final String file = files[i % files.length];
        final long size = sizes[i % sizes.length];
        final long total = totals[i % totals.length];

        exec.submit(new Callable<Void>() {
          @Override
          public Void call() {
            startupProgress.beginPhase(phase);
            startupProgress.setFile(phase, file);
            startupProgress.setSize(phase, size);
            startupProgress.setTotal(phase, step, total);
            incrementCounter(startupProgress, phase, step, 100L);
            startupProgress.endStep(phase, step);
            return null;
          }
        });
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(10000L, TimeUnit.MILLISECONDS));
    }
    // Once a phase ends, future modifications to the steps in that phase are
    // ignored. Thus do not end the phases until after the other ops are done.
    for (Phase phase : phases) {
      startupProgress.endPhase(phase);
    }

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals("file1", view.getFile(LOADING_FSIMAGE));
    assertEquals(1000L, view.getSize(LOADING_FSIMAGE));
    assertEquals(10000L, view.getTotal(LOADING_FSIMAGE, new Step(INODES)));
    assertEquals(2500L, view.getCount(LOADING_FSIMAGE, new Step(INODES)));
    assertEquals(20000L, view.getTotal(LOADING_FSIMAGE,
      new Step(DELEGATION_KEYS)));
    assertEquals(2500L, view.getCount(LOADING_FSIMAGE,
      new Step(DELEGATION_KEYS)));

    assertEquals("file2", view.getFile(LOADING_EDITS));
    assertEquals(2000L, view.getSize(LOADING_EDITS));
    assertEquals(30000L, view.getTotal(LOADING_EDITS, new Step(INODES)));
    assertEquals(2500L, view.getCount(LOADING_EDITS, new Step(INODES)));
    assertEquals(40000L, view.getTotal(LOADING_EDITS,
      new Step(DELEGATION_KEYS)));
    assertEquals(2500L, view.getCount(LOADING_EDITS, new Step(DELEGATION_KEYS)));
  }

  @Test(timeout=10000)
  public void testTotal() {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    Step loadingFsImageInodes = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageInodes);
    startupProgress.setTotal(LOADING_FSIMAGE, loadingFsImageInodes, 1000L);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Step loadingFsImageDelegationKeys = new Step(DELEGATION_KEYS);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.setTotal(LOADING_FSIMAGE, loadingFsImageDelegationKeys,
      800L);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.endPhase(LOADING_FSIMAGE);

    startupProgress.beginPhase(LOADING_EDITS);
    Step loadingEditsFile = new Step("file", 1000L);
    startupProgress.beginStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.setTotal(LOADING_EDITS, loadingEditsFile, 10000L);
    startupProgress.endStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.endPhase(LOADING_EDITS);

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(1000L, view.getTotal(LOADING_FSIMAGE, loadingFsImageInodes));
    assertEquals(800L, view.getTotal(LOADING_FSIMAGE,
      loadingFsImageDelegationKeys));
    assertEquals(10000L, view.getTotal(LOADING_EDITS, loadingEditsFile));

    // Try adding another step to the completed phase
    // Check the step is not added and the total is not updated
    Step step2 = new Step("file_2", 7000L);
    startupProgress.setTotal(LOADING_EDITS, step2, 2000L);
    view = startupProgress.createView();
    assertEquals(view.getTotal(LOADING_EDITS, step2), 0);
    Counter counter = startupProgress.getCounter(Phase.LOADING_EDITS, step2);
    counter.increment();
    assertEquals(view.getCount(LOADING_EDITS, step2), 0);
  }
}
