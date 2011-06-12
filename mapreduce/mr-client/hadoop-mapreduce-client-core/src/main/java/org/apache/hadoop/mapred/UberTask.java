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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.TaskType;  // MAP, JOB_SETUP, TASK_CLEANUP...
import org.apache.hadoop.util.Progress;

class UberTask extends Task {
  private TaskSplitIndex[] splits;
  private int numMapTasks;
  private int numReduceTasks;
  private boolean jobSetupCleanupNeeded;

  private static final Log LOG = LogFactory.getLog(UberTask.class.getName());

  private Progress[] subPhases;  // persistent storage for MapTasks, ReduceTask

  // "instance initializer":  executed between Task and UberTask constructors
  {
    // cannot call setPhase() here now that createTaskStatus() is called in
    // Task subclass(es):  initializer is executed before subclass ctor =>
    // taskStatus still null => NPE
    getProgress().setStatus("uber"); // Task.java: change name of root Progress
  }

  public UberTask() {
    super();
    this.taskStatus = new UberTaskStatus();
  }

  public UberTask(String jobFile, TaskAttemptID taskId, int partition,
                  TaskSplitIndex[] splits, int numReduceTasks,
                  int numSlotsRequired, boolean jobSetupCleanupNeeded) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.splits = splits;
    this.numMapTasks = splits.length;
    this.numReduceTasks = numReduceTasks;
    this.jobSetupCleanupNeeded = jobSetupCleanupNeeded;
    this.taskStatus = new UberTaskStatus(getTaskID(), 0.0f, numSlotsRequired,
                                         TaskStatus.State.UNASSIGNED,
                                         "", "", "", TaskStatus.Phase.MAP,
                                         getCounters());
    if (LOG.isDebugEnabled()) {
      LOG.debug("UberTask " + getTaskID() + " constructed with " + numMapTasks
                + " sub-maps and " + numReduceTasks + " sub-reduces");
    }
  }

  /* perhaps someday we'll allow an UberTask to run as either a MapTask or a
   * ReduceTask, but for now it's the latter only */
  @Override
  public boolean isMapTask() {
    return false;
  }

  /**
   * Is this really a combo-task masquerading as a plain ReduceTask?  Yup.
   */
  @Override
  public boolean isUberTask() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, TaskUmbilicalProtocol umbilical)
  throws IOException, ClassNotFoundException, InterruptedException {
    this.umbilical = umbilical;

    // set up two-level Progress/phase tree:  getProgress() is root ("uber"),
    // and subtasks' "root node" Progress is second level (will override
    // native one when construct each subtask)
    subPhases = new Progress[numMapTasks + numReduceTasks];
    for (int j=0; j < numMapTasks; ++j) {
      subPhases[j] = getProgress().addPhase("map " + String.valueOf(j+1));
    }
    for (int j = numMapTasks; j < numMapTasks + numReduceTasks; ++j) {
      subPhases[j] =
        getProgress().addPhase("reduce " + String.valueOf(j - numMapTasks + 1));
    }
    // we could set up each subtask's phases, too, but would need to store all
    // (2*numMapTasks + 2*numReduceTasks) of them here, and subtasks already
    // have storage allocated (mapPhase, sortPhase, copyPhase, reducePhase) in
    // MapTask and ReduceTask--instead, will call new accessor for each after
    // each subtask is created

    // Start thread that will handle communication with parent.  Note that this
    // is NOT the reporter the subtasks will use--we want them to get one that
    // knows nothing of umbilical, so that calls to it will pass through us,
    // changing the task ID to our own (UberTask's) before sending progress on
    // up via this reporter.  (No need for the subtask reporter also to adjust
    // the progress percentage; we get that for free from the phase tree.)
    TaskReporter reporter = startReporter(umbilical);

    // use context objects API?
    boolean useNewApi = job.getUseNewMapper();   // "mapred.mapper.new-api"
    assert useNewApi == job.getUseNewReducer();  // enforce consistency

    // initialize the ubertask (sole "real" task as far as framework is
    // concerned); this is where setupTask() is called
    initialize(job, getJobID(), reporter, useNewApi);

    // Generate the map TaskAttemptIDs we need to run.
    // Regular tasks are handed their TaskAttemptIDs via TaskInProgress's
    // getTaskToRun() and addRunningTask(), but that approach doesn't work
    // for us.  Ergo, we create our own--technically bypassing the nextTaskId
    // limits in getTaskToRun(), but since the point of UberTask is to roll
    // up too-small jobs into a single, more normal-sized ubertask (whose
    // single TaskAttemptID _is_ subject to the limits), that's reasonable.
    TaskAttemptID[] mapIds = createMapIds();

    // set up the job
    if (jobSetupCleanupNeeded) {
      runSetupJob(reporter);
    }

    // run the maps
    if (numMapTasks > 0) {
      runUberMapTasks(job, mapIds, splits, umbilical, reporter);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("UberTask " + getTaskID() + " has no sub-MapTasks to run");
      }
    }

    if (numReduceTasks > 0) {
      // may make sense to lift this restriction at some point, but for now
      // code is written to support one at most:
      if (numReduceTasks > 1) {
        throw new IOException("UberTask invoked with " + numReduceTasks
                              + " reduces (1 max)");
      }

      // set up the reduce ...
      Class keyClass = job.getMapOutputKeyClass();
      Class valueClass = job.getMapOutputValueClass();
      RawComparator comparator = job.getOutputValueGroupingComparator();

      // ... then run it (using our own [reduce] TaskAttemptID)
      runUberReducer(job, getTaskID(), mapIds.length, umbilical, reporter,
                     comparator, keyClass, valueClass);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("UberTask " + getTaskID() + " has no sub-ReduceTasks to run");
      }
    }

    // clean up the job (switch phase to "cleanup" and delete staging dir, but
    // do NOT delete temp dir yet)
    if (jobSetupCleanupNeeded) {
      runCommitAbortJob(reporter);
    }

    // this is where commitTask() (or abortTask()) is called
    done(umbilical, reporter);

    // now finish cleaning up the job (delete temp dir:  results are committed)
    if (jobSetupCleanupNeeded) {
      commitJob();
    }
  }

  private TaskAttemptID[] createMapIds() {
    TaskAttemptID[] mapIds = new TaskAttemptID[numMapTasks];
    // Note that the reducer always starts looking for ID 0 (output/map_0.out),
    // so it's not possible (or at least not easy) to add an offset to the
    // mapIds.  However, since ~nobody but us ever sees them (thanks to
    // SubTaskReporter translation below--progress is reported to TT using
    // UberTask's ID), it's OK to overlap our own task ID and potentially
    // those of the setup and cleanup tasks.
    for (int j = 0; j < mapIds.length; ++j) {
      mapIds[j] = new TaskAttemptID(new TaskID(getJobID(), TaskType.MAP, j), 0);
    }
    return mapIds;
  }

  /**
   * Within the _local_ filesystem (not HDFS), all activity takes place within
   * a single directory (e.g., "/tmp/hadoop-[username]/mapred/local/0_0/
   * taskTracker/[username]/jobcache/job_xxx/attempt_xxx_r_xxx/output/"), and
   * all sub-MapTasks create the same filename ("file.out").  Rename that to
   * something unique (e.g., "map_0.out") to avoid collisions.
   *
   * Longer-term, we'll modify TaskTracker or whatever to use TaskAttemptID-
   * based filenames instead of "file.out".  (All of this is entirely internal,
   * so there are no particular compatibility issues.)
   */
  private void renameMapOutputForReduce(TaskAttemptID mapId,
                                        MapOutputFile subMapOutputFile)
  throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf); //PERF FIXME? could pass in
    // move map output to reduce input
    Path mapOut = subMapOutputFile.getOutputFile();
    Path reduceIn = subMapOutputFile.getInputFileForWrite(
        mapId.getTaskID(), localFs.getLength(mapOut));
    if (!localFs.mkdirs(reduceIn.getParent())) {
      throw new IOException("Mkdirs failed to create "
          + reduceIn.getParent().toString());
    }
    if (!localFs.rename(mapOut, reduceIn))
      throw new IOException("Couldn't rename " + mapOut);
  }

  private void runSetupJob(TaskReporter reporter)
  throws IOException, InterruptedException {
    runJobSetupTask(umbilical, reporter);
  }

  private void runCommitAbortJob(TaskReporter reporter)
  throws IOException, InterruptedException {
    // if we (uber) got this far without _ourselves_ being whacked, then we've
    // succeeded
    setJobCleanupTaskState(org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED);
    runJobCleanupTask(umbilical, reporter);
  }

  /**
   * This is basically an uber-specific version of MapTask's run() method.
   * It loops over the map subtasks sequentially.  runUberReducer() (below)
   * is the corresponding replacement for ReduceTask's run().
   */
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runUberMapTasks(final JobConf job,
                       final TaskAttemptID[] mapIds,
                       final TaskSplitIndex[] splits,
                       final TaskUmbilicalProtocol umbilical,
                       TaskReporter reporter)
  throws IOException, InterruptedException, ClassNotFoundException {
    boolean useNewApi = job.getUseNewMapper();  // use context objects API?

    for (int j=0; j < mapIds.length; ++j) {
      MapTask map = new MapTask(getJobFile(), mapIds[j], j, splits[j], 1);
      JobConf localConf = new JobConf(job);
      map.localizeConfiguration(localConf);
      map.setConf(localConf);
      // for reporting purposes (to TT), use uber's task ID, not subtask's:
      map.setTaskIdForUmbilical(getTaskID());

      // override MapTask's "root" Progress node with our second-level one...
      map.setProgress(subPhases[j]);
      // ...and add two third-level Progress nodes
      map.createPhase(TaskStatus.Phase.MAP, "map", 0.667f);
      map.createPhase(TaskStatus.Phase.SORT, "sort", 0.333f);

      TaskReporter subReporter =
          new SubTaskReporter(map.getProgress(), reporter, j);
      map.initialize(localConf, getJobID(), subReporter, useNewApi);

      LOG.info("UberTask " + getTaskID() + " running sub-MapTask " + (j+1)
               + "/" + numMapTasks);

      if (useNewApi) {
        MapTask.runNewMapper(map, localConf, splits[j], umbilical, subReporter);
      } else {
        MapTask.runOldMapper(map, localConf, splits[j], umbilical, subReporter);
      }
      updateCounters(map);

      // Set own progress to 1.0 and move to next sibling node in Progress/phase
      // tree.  NOTE:  this works but is slightly fragile.  Sibling doesn't
      // yet exist, but since internal startNextPhase() call merely updates
      // currentPhase index without "dereferencing" it, this is OK as long as
      // no one calls phase() on parent Progress (or get()?) in interim.
      map.getProgress().complete();

      // Even for M+R jobs, we need to save (commit) each map's output (since
      // user may create save-worthy side-files in the work/tempdir), which
      // usually entails asking the TT for permission (because of speculation)
      // and then moving it up one subdirectory level in HDFS (i.e., out of
      // _temporary/_attempt_xxx).  However, the TT gives permission only if
      // the JT sent a commitAction for the task, which it hasn't yet done
      // for UberTask and which it will never do for uber-subtasks of which
      // it knows nothing.  Therefore we just do the two-subdir thing and
      // make sure elsewhere that speculation is never on for UberTasks.
      // Use UberTask's reporter so we set the progressFlag to which the
      // communication thread is paying attention; it has no knowledge of
      // subReporter.
      map.commit(umbilical, reporter);  // includes "reporter.progress()"

      // Every map will produce "file.out" in the same (local, not HDFS!) dir,
      // so rename to "map_#.out" as we go.  (Longer-term, will use
      // TaskAttemptIDs as part of name => avoid rename.)  Note that this has
      // nothing to do with the _temporary/_attempt_xxx _HDFS_ subdir above!
      if (numReduceTasks > 0) {
        renameMapOutputForReduce(mapIds[j], map.getMapOutputFile());
      }
    }
  }

  /**
   * This is basically an uber-specific version of ReduceTask's run() method.
   * It currently supports only a single reducer (or none, in the trivial sense
   * of not being called in that case).
   */
  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runUberReducer(JobConf job, TaskAttemptID reduceId, int numMaps,
                      final TaskUmbilicalProtocol umbilical,
                      final TaskReporter reporter,
                      RawComparator<INKEY> comparator,
                      Class<INKEY> keyClass,
                      Class<INVALUE> valueClass)
  throws IOException, InterruptedException, ClassNotFoundException {
    boolean useNewApi = job.getUseNewReducer();  // use context objects API?
    ReduceTask reduce = new ReduceTask(getJobFile(), reduceId, 0, numMaps, 1);
    JobConf localConf = new JobConf(job);
    reduce.localizeConfiguration(localConf);
    reduce.setConf(localConf);
    localConf.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    // override ReduceTask's "root" Progress node with our second-level one...
    reduce.setProgress( subPhases[numMapTasks+0] );
    // ...and add two third-level Progress nodes (SHUFFLE/"copy" is unnecessary)
    reduce.createPhase(TaskStatus.Phase.SORT, "sort");
    reduce.createPhase(TaskStatus.Phase.REDUCE, "reduce");

    // subtaskIndex of reduce is one bigger than that of last map,
    // i.e., (numMapTasks-1) + 1
    TaskReporter subReporter =
        new SubTaskReporter(reduce.getProgress(), reporter, numMapTasks);
    reduce.initialize(localConf, getJobID(), subReporter, useNewApi);

    LOG.info("UberTask " + getTaskID() + " running sub-ReduceTask 1/"
             + numReduceTasks);

    // note that this is implicitly the "isLocal" branch of ReduceTask run():
    // we don't have a shuffle phase
    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    RawKeyValueIterator rIter =
        Merger.merge(job, rfs, job.getMapOutputKeyClass(),
                     job.getMapOutputValueClass(), reduce.initCodec(localConf),
                     ReduceTask.getMapFiles(reduce, rfs, true),
                     !conf.getKeepFailedTaskFiles(),
                     job.getInt(JobContext.IO_SORT_FACTOR, 100),
                     new Path(getTaskID().toString()),
                     job.getOutputKeyComparator(),
                     subReporter, spilledRecordsCounter,
                     null, null);   // no writesCounter or mergePhase

    // set progress = 1.0 and move _parent's_ index to next sibling phase:
    reduce.completePhase(TaskStatus.Phase.SORT);  // "sortPhase.complete()"
    reduce.taskStatus.setPhase(TaskStatus.Phase.REDUCE);

    if (useNewApi) {
      ReduceTask.runNewReducer(reduce, job, umbilical, subReporter,
                               rIter, comparator, keyClass, valueClass);
    } else {
      ReduceTask.runOldReducer(reduce, job, umbilical, subReporter,
                               rIter, comparator, keyClass, valueClass);
    }
    updateCounters(reduce);

    // set own progress to 1.0 and move to [nonexistent] next sibling node in
    // Progress/phase tree; this will cause parent node's progress (UberTask's)
    // to be set to 1.0, too (at least, assuming all previous siblings have
    // done so, too...Progress/phase stuff is fragile in more ways than one)
    reduce.getProgress().complete();

    // signal the communication thread to pass any progress on up to the TT
    // [There's no explicit reduce.commit() because we're reusing ubertask's
    // ID and temp dir => ubertask's commit() will take care of us.  But if
    // we ever support more than one reduce, we'll have to do explicit sub-
    // commit() as with maps above.]
    reporter.progress();
  }

  /**
   * Updates uber-counters with values from completed subtasks.
   * @param subtask  a map or reduce subtask that has just been successfully
   *                 completed
   */
  private void updateCounters(Task subtask) {
    Counters counters = getCounters();
    if (counters != null) {
      counters.incrCounter(subtask.isMapTask()?
          JobCounter.NUM_UBER_SUBMAPS : JobCounter.NUM_UBER_SUBREDUCES, 1);
      counters.incrAllCounters(subtask.getCounters());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (isMapOrReduce()) {
      out.writeBoolean(jobSetupCleanupNeeded);
      WritableUtils.writeVInt(out, numMapTasks);
      WritableUtils.writeVInt(out, numReduceTasks);
      for (TaskSplitIndex splitIndex : splits) {
        splitIndex.write(out);
      }
      splits = null;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (isMapOrReduce()) {
      jobSetupCleanupNeeded = in.readBoolean();
      numMapTasks = WritableUtils.readVInt(in);
      numReduceTasks = WritableUtils.readVInt(in);
      splits = new TaskSplitIndex[numMapTasks];
      for (int j=0; j < numMapTasks; ++j) {
        TaskSplitIndex splitIndex = new TaskSplitIndex();
        splitIndex.readFields(in);
        splits[j] = splitIndex;
      }
    }
  }

  /**
   * In our superclass, the communication thread handles communication with
   * the parent (TaskTracker) via the umbilical, but that works only if the
   * TaskTracker is aware of the task's ID--which is true of us (UberTask)
   * but not our map and reduce subtasks.  Ergo, intercept subtasks' progress
   * reports and pass them on as our own, i.e., use our own uber-taskID in
   * place of the subtasks' bogus ones.  (Leave the progress percentage alone;
   * the phase/Progress hierarchy we set up in run() and runUber*() will take
   * care of that.)
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  protected class SubTaskReporter extends Task.TaskReporter
  implements Runnable, Reporter {

    private Progress taskProgress;
    private TaskReporter uberReporter;
    private int subtaskIndex;
    // subtaskIndex index goes from 0 -> (m-1+r), where m = number of maps and
    // r = number of reduces.  (Latter can be either 0 or 1, and m+r >= 1.)

    SubTaskReporter(Progress progress, TaskReporter reporter, int subtaskIdx) {
      super(progress, null);
      this.taskProgress = progress;
      this.uberReporter = reporter;
      this.subtaskIndex = subtaskIdx;
    }

    @Override
    public void run() {
      // make sure this never gets called...
      LOG.fatal("UberTask " + getTaskID() + " SubTaskReporter run() called "
                + "unexpectedly for subtask index " + subtaskIndex);
      assert "uh oh:  SubTaskReporter's run() method was called!".isEmpty();
    }

    // just one (real) intercepted method
    @Override
    public void setProgress(float progress) {
      // update _our_ taskProgress [no need to do uber's, too:  ultimately does
      // get() on uber's taskProgress], but set _uberReporter's_ progress flag
      taskProgress.phase().set(progress);
      uberReporter.setProgressFlag();
    }
  }

}
