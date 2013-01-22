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

package org.apache.hadoop.mapreduce.v2.app.commit;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobAbortCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobSetupCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobSetupFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class CommitterEventHandler extends AbstractService
    implements EventHandler<CommitterEvent> {

  private static final Log LOG =
      LogFactory.getLog(CommitterEventHandler.class);

  private final AppContext context;
  private final OutputCommitter committer;
  private final RMHeartbeatHandler rmHeartbeatHandler;
  private ThreadPoolExecutor launcherPool;
  private Thread eventHandlingThread;
  private BlockingQueue<CommitterEvent> eventQueue =
      new LinkedBlockingQueue<CommitterEvent>();
  private final AtomicBoolean stopped;
  private Thread jobCommitThread = null;
  private int commitThreadCancelTimeoutMs;
  private long commitWindowMs;
  private FileSystem fs;
  private Path startCommitFile;
  private Path endCommitSuccessFile;
  private Path endCommitFailureFile;
  

  public CommitterEventHandler(AppContext context, OutputCommitter committer,
      RMHeartbeatHandler rmHeartbeatHandler) {
    super("CommitterEventHandler");
    this.context = context;
    this.committer = committer;
    this.rmHeartbeatHandler = rmHeartbeatHandler;
    this.stopped = new AtomicBoolean(false);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    commitThreadCancelTimeoutMs = conf.getInt(
        MRJobConfig.MR_AM_COMMITTER_CANCEL_TIMEOUT_MS,
        MRJobConfig.DEFAULT_MR_AM_COMMITTER_CANCEL_TIMEOUT_MS);
    commitWindowMs = conf.getLong(MRJobConfig.MR_AM_COMMIT_WINDOW_MS,
        MRJobConfig.DEFAULT_MR_AM_COMMIT_WINDOW_MS);
    try {
      fs = FileSystem.get(conf);
      JobID id = TypeConverter.fromYarn(context.getApplicationID());
      JobId jobId = TypeConverter.toYarn(id);
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
      endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, jobId);
      endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, jobId);
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  @Override
  public void start() {    
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("CommitterEvent Processor #%d")
      .build();
    launcherPool = new ThreadPoolExecutor(5, 5, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        CommitterEvent event = null;
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }
          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(new EventProcessor(event));        }
      }
    });
    eventHandlingThread.setName("CommitterEvent Handler");
    eventHandlingThread.start();
    super.start();
  }


  @Override
  public void handle(CommitterEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  @Override
  public void stop() {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    eventHandlingThread.interrupt();
    launcherPool.shutdown();
    super.stop();
  }

  private synchronized void jobCommitStarted() throws IOException {
    if (jobCommitThread != null) {
      throw new IOException("Commit while another commit thread active: "
          + jobCommitThread.toString());
    }

    jobCommitThread = Thread.currentThread();
  }

  private synchronized void jobCommitEnded() {
    if (jobCommitThread == Thread.currentThread()) {
      jobCommitThread = null;
      notifyAll();
    }
  }

  private synchronized void cancelJobCommit() {
    Thread threadCommitting = jobCommitThread;
    if (threadCommitting != null && threadCommitting.isAlive()) {
      LOG.info("Canceling commit");
      threadCommitting.interrupt();

      // wait up to configured timeout for commit thread to finish
      long now = context.getClock().getTime();
      long timeoutTimestamp = now + commitThreadCancelTimeoutMs;
      try {
        while (jobCommitThread == threadCommitting
            && now > timeoutTimestamp) {
          wait(now - timeoutTimestamp);
          now = context.getClock().getTime();
        }
      } catch (InterruptedException e) {
      }
    }
  }

  private class EventProcessor implements Runnable {
    private CommitterEvent event;

    EventProcessor(CommitterEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());
      switch (event.getType()) {
      case JOB_SETUP:
        handleJobSetup((CommitterJobSetupEvent) event);
        break;
      case JOB_COMMIT:
        handleJobCommit((CommitterJobCommitEvent) event);
        break;
      case JOB_ABORT:
        handleJobAbort((CommitterJobAbortEvent) event);
        break;
      case TASK_ABORT:
        handleTaskAbort((CommitterTaskAbortEvent) event);
        break;
      default:
        throw new YarnException("Unexpected committer event "
            + event.toString());
      }
    }
    
    @SuppressWarnings("unchecked")
    protected void handleJobSetup(CommitterJobSetupEvent event) {
      try {
        committer.setupJob(event.getJobContext());
        context.getEventHandler().handle(
            new JobSetupCompletedEvent(event.getJobID()));
      } catch (Exception e) {
        LOG.warn("Job setup failed", e);
        context.getEventHandler().handle(new JobSetupFailedEvent(
            event.getJobID(), StringUtils.stringifyException(e)));
      }
    }

    private void touchz(Path p) throws IOException {
      fs.create(p, false).close();
    }
    
    @SuppressWarnings("unchecked")
    protected void handleJobCommit(CommitterJobCommitEvent event) {
      try {
        touchz(startCommitFile);
        jobCommitStarted();
        waitForValidCommitWindow();
        committer.commitJob(event.getJobContext());
        touchz(endCommitSuccessFile);
        context.getEventHandler().handle(
            new JobCommitCompletedEvent(event.getJobID()));
      } catch (Exception e) {
        try {
          touchz(endCommitFailureFile);
        } catch (Exception e2) {
          LOG.error("could not create failure file.", e2);
        }
        LOG.error("Could not commit job", e);
        context.getEventHandler().handle(
            new JobCommitFailedEvent(event.getJobID(),
                StringUtils.stringifyException(e)));
      } finally {
        jobCommitEnded();
      }
    }

    @SuppressWarnings("unchecked")
    protected void handleJobAbort(CommitterJobAbortEvent event) {
      cancelJobCommit();

      try {
        committer.abortJob(event.getJobContext(), event.getFinalState());
      } catch (Exception e) {
        LOG.warn("Could not abort job", e);
      }

      context.getEventHandler().handle(new JobAbortCompletedEvent(
          event.getJobID(), event.getFinalState()));
    }

    @SuppressWarnings("unchecked")
    protected void handleTaskAbort(CommitterTaskAbortEvent event) {
      try {
        committer.abortTask(event.getAttemptContext());
      } catch (Exception e) {
        LOG.warn("Task cleanup failed for attempt " + event.getAttemptID(), e);
      }
      context.getEventHandler().handle(
          new TaskAttemptEvent(event.getAttemptID(),
              TaskAttemptEventType.TA_CLEANUP_DONE));
    }

    private synchronized void waitForValidCommitWindow()
        throws InterruptedException {
      long lastHeartbeatTime = rmHeartbeatHandler.getLastHeartbeatTime();
      long now = context.getClock().getTime();

      while (now - lastHeartbeatTime > commitWindowMs) {
        rmHeartbeatHandler.runOnNextHeartbeat(new Runnable() {
          @Override
          public void run() {
            synchronized (EventProcessor.this) {
              EventProcessor.this.notify();
            }
          }
        });

        wait();
        lastHeartbeatTime = rmHeartbeatHandler.getLastHeartbeatTime();
        now = context.getClock().getTime();
      }
    }
  }
}
