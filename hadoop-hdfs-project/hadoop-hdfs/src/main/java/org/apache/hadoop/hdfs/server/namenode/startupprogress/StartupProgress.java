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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * StartupProgress is used in various parts of the namenode codebase to indicate
 * startup progress.  Its methods provide ways to indicate begin and end of a
 * {@link Phase} or {@link Step} within a phase.  Additional methods provide ways
 * to associate a step or phase with optional information, such as a file name or
 * file size.  It also provides counters, which can be incremented by the  caller
 * to indicate progress through a long-running task.
 * 
 * This class is thread-safe.  Any number of threads may call any methods, even
 * for the same phase or step, without risk of corrupting internal state.  For
 * all begin/end methods and set methods, the last one in wins, overwriting any
 * prior writes.  Instances of {@link Counter} provide an atomic increment
 * operation to prevent lost updates.
 * 
 * After startup completes, the tracked data is frozen.  Any subsequent updates
 * or counter increments are no-ops.
 * 
 * For read access, call {@link #createView()} to create a consistent view with
 * a clone of the data.
 */
@InterfaceAudience.Private
public class StartupProgress {
  // package-private for access by StartupProgressView
  final Map<Phase, PhaseTracking> phases =
    new ConcurrentHashMap<Phase, PhaseTracking>();

  /**
   * Allows a caller to increment a counter for tracking progress.
   */
  public static interface Counter {
    /**
     * Atomically increments this counter, adding 1 to the current value.
     */
    void increment();
  }

  /**
   * Creates a new StartupProgress by initializing internal data structure for
   * tracking progress of all defined phases.
   */
  public StartupProgress() {
    for (Phase phase: EnumSet.allOf(Phase.class)) {
      phases.put(phase, new PhaseTracking());
    }
  }

  /**
   * Begins execution of the specified phase.
   * 
   * @param phase Phase to begin
   */
  public void beginPhase(Phase phase) {
    if (!isComplete()) {
      phases.get(phase).beginTime = monotonicNow();
    }
  }

  /**
   * Begins execution of the specified step within the specified phase. This is
   * a no-op if the phase is already completed.
   * 
   * @param phase Phase within which the step should be started
   * @param step Step to begin
   */
  public void beginStep(Phase phase, Step step) {
    if (!isComplete(phase)) {
      lazyInitStep(phase, step).beginTime = monotonicNow();
    }
  }

  /**
   * Ends execution of the specified phase.
   * 
   * @param phase Phase to end
   */
  public void endPhase(Phase phase) {
    if (!isComplete()) {
      phases.get(phase).endTime = monotonicNow();
    }
  }

  /**
   * Ends execution of the specified step within the specified phase. This is
   * a no-op if the phase is already completed.
   *
   * @param phase Phase within which the step should be ended
   * @param step Step to end
   */
  public void endStep(Phase phase, Step step) {
    if (!isComplete(phase)) {
      lazyInitStep(phase, step).endTime = monotonicNow();
    }
  }

  /**
   * Returns the current run status of the specified phase.
   * 
   * @param phase Phase to get
   * @return Status run status of phase
   */
  public Status getStatus(Phase phase) {
    PhaseTracking tracking = phases.get(phase);
    if (tracking.beginTime == Long.MIN_VALUE) {
      return Status.PENDING;
    } else if (tracking.endTime == Long.MIN_VALUE) {
      return Status.RUNNING;
    } else {
      return Status.COMPLETE;
    }
  }

  /**
   * Returns a counter associated with the specified phase and step.  Typical
   * usage is to increment a counter within a tight loop.  Callers may use this
   * method to obtain a counter once and then increment that instance repeatedly
   * within a loop.  This prevents redundant lookup operations and object
   * creation within the tight loop.  Incrementing the counter is an atomic
   * operation, so there is no risk of lost updates even if multiple threads
   * increment the same counter.
   * 
   * @param phase Phase to get
   * @param step Step to get
   * @return Counter associated with phase and step
   */
  public Counter getCounter(Phase phase, Step step) {
    if (!isComplete(phase)) {
      final StepTracking tracking = lazyInitStep(phase, step);
      return new Counter() {
        @Override
        public void increment() {
          tracking.count.incrementAndGet();
        }
      };
    } else {
      return new Counter() {
        @Override
        public void increment() {
          // no-op, because startup has completed
        }
      };
    }
  }

  /**
   * Sets counter to the specified value.
   * 
   * @param phase Phase to set
   * @param step Step to set
   * @param count long to set
   */
  public void setCount(Phase phase, Step step, long count) {
    lazyInitStep(phase, step).count.set(count);
  }

  /**
   * Sets the optional file name associated with the specified phase.  For
   * example, this can be used while loading fsimage to indicate the full path to
   * the fsimage file.
   * 
   * @param phase Phase to set
   * @param file String file name to set
   */
  public void setFile(Phase phase, String file) {
    if (!isComplete()) {
      phases.get(phase).file = file;
    }
  }

  /**
   * Sets the optional size in bytes associated with the specified phase.  For
   * example, this can be used while loading fsimage to indicate the size of the
   * fsimage file.
   * 
   * @param phase Phase to set
   * @param size long to set
   */
  public void setSize(Phase phase, long size) {
    if (!isComplete()) {
      phases.get(phase).size = size;
    }
  }

  /**
   * Sets the total associated with the specified phase and step.  For example,
   * this can be used while loading edits to indicate the number of operations to
   * be applied.
   * 
   * @param phase Phase to set
   * @param step Step to set
   * @param total long to set
   */
  public void setTotal(Phase phase, Step step, long total) {
    if (!isComplete(phase)) {
      lazyInitStep(phase, step).total = total;
    }
  }

  /**
   * Creates a {@link StartupProgressView} containing data cloned from this
   * StartupProgress.  Subsequent updates to this StartupProgress will not be
   * shown in the view.  This gives a consistent, unchanging view for callers
   * that need to perform multiple related read operations.  Calculations that
   * require aggregation, such as overall percent complete, will not be impacted
   * by mutations performed in other threads mid-way through the calculation.
   * 
   * @return StartupProgressView containing cloned data
   */
  public StartupProgressView createView() {
    return new StartupProgressView(this);
  }

  /**
   * Returns true if the entire startup process has completed, determined by
   * checking if each phase is complete.
   * 
   * @return boolean true if the entire startup process has completed
   */
  private boolean isComplete() {
    return EnumSet.allOf(Phase.class).stream().allMatch(this::isComplete);
  }

  /**
   * Returns true if the given startup phase has been completed.
   *
   * @param phase Which phase to check for completion
   * @return boolean true if the given startup phase has completed.
   */
  private boolean isComplete(Phase phase) {
    return getStatus(phase) == Status.COMPLETE;
  }

  /**
   * Lazily initializes the internal data structure for tracking the specified
   * phase and step.  Returns either the newly initialized data structure or the
   * existing one.  Initialization is atomic, so there is no risk of lost updates
   * even if multiple threads attempt to initialize the same step simultaneously.
   * 
   * @param phase Phase to initialize
   * @param step Step to initialize
   * @return StepTracking newly initialized, or existing if found
   */
  private StepTracking lazyInitStep(Phase phase, Step step) {
    ConcurrentMap<Step, StepTracking> steps = phases.get(phase).steps;
    if (!steps.containsKey(step)) {
      steps.putIfAbsent(step, new StepTracking());
    }
    return steps.get(step);
  }
}
