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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;

/**
 * Component accepting submitted, running {@link Statistics.JobStats} and 
 * responsible for monitoring jobs for success and failure. Once a job is 
 * submitted, it is polled for status until complete. If a job is complete, 
 * then the monitor thread returns immediately to the queue. If not, the monitor
 * will sleep for some duration.
 * 
 * {@link JobMonitor} can be configured to use multiple threads for polling
 * the job statuses. Use {@link Gridmix#GRIDMIX_JOBMONITOR_THREADS} to specify
 * the total number of monitoring threads. 
 * 
 * The duration for which a monitoring thread sleeps if the first job in the 
 * queue is running can also be configured. Use 
 * {@link Gridmix#GRIDMIX_JOBMONITOR_SLEEPTIME_MILLIS} to specify a custom 
 * value.
 */
class JobMonitor implements Gridmix.Component<JobStats> {

  public static final Log LOG = LogFactory.getLog(JobMonitor.class);

  private final Queue<JobStats> mJobs;
  private ExecutorService executor;
  private int numPollingThreads;
  private final BlockingQueue<JobStats> runningJobs;
  private final long pollDelayMillis;
  private Statistics statistics;
  private boolean graceful = false;
  private boolean shutdown = false;

  /**
   * Create a JobMonitor that sleeps for the specified duration after
   * polling a still-running job.
   * @param pollDelay Delay after polling a running job
   * @param unit Time unit for pollDelaySec (rounded to milliseconds)
   * @param statistics StatCollector , listener to job completion.
   */
  public JobMonitor(int pollDelay, TimeUnit unit, Statistics statistics, 
                    int numPollingThreads) {
    executor = Executors.newCachedThreadPool();
    this.numPollingThreads = numPollingThreads;
    runningJobs = new LinkedBlockingQueue<JobStats>();
    mJobs = new LinkedList<JobStats>();
    this.pollDelayMillis = TimeUnit.MILLISECONDS.convert(pollDelay, unit);
    this.statistics = statistics;
  }

  /**
   * Add a running job's status to the polling queue.
   */
  public void add(JobStats job) throws InterruptedException {
      runningJobs.put(job);
  }

  /**
   * Add a submission failed job's status, such that it can be communicated
   * back to serial.
   * TODO: Cleaner solution for this problem
   * @param job
   */
  public void submissionFailed(JobStats job) {
    String jobID = job.getJob().getConfiguration().get(Gridmix.ORIGINAL_JOB_ID);
    LOG.info("Job submission failed notification for job " + jobID);
    synchronized (statistics) {
      this.statistics.add(job);
    }
  }

  /**
   * Temporary hook for recording job success.
   */
  protected void onSuccess(Job job) {
    LOG.info(job.getJobName() + " (" + job.getJobID() + ")" + " success");
  }

  /**
   * Temporary hook for recording job failure.
   */
  protected void onFailure(Job job) {
    LOG.info(job.getJobName() + " (" + job.getJobID() + ")" + " failure");
  }

  /**
   * If shutdown before all jobs have completed, any still-running jobs
   * may be extracted from the component.
   * @throws IllegalStateException If monitoring thread is still running.
   * @return Any jobs submitted and not known to have completed.
   */
  List<JobStats> getRemainingJobs() {
    synchronized (mJobs) {
      return new ArrayList<JobStats>(mJobs);
    }
  }

  /**
   * Monitoring thread pulling running jobs from the component and into
   * a queue to be polled for status.
   */
  private class MonitorThread extends Thread {

    public MonitorThread(int i) {
      super("GridmixJobMonitor-" + i);
    }

    @Override
    public void run() {
      boolean graceful;
      boolean shutdown;
      while (true) {
        try {
          synchronized (mJobs) {
            graceful = JobMonitor.this.graceful;
            shutdown = JobMonitor.this.shutdown;
            runningJobs.drainTo(mJobs);
          }

          // shutdown conditions; either shutdown requested and all jobs
          // have completed or abort requested and there are recently
          // submitted jobs not in the monitored set
          if (shutdown) {
            if (!graceful) {
              while (!runningJobs.isEmpty()) {
                synchronized (mJobs) {
                  runningJobs.drainTo(mJobs);
                }
              }
              break;
            }
            
            synchronized (mJobs) {
              if (graceful && mJobs.isEmpty()) {
                break;
              }
            }
          }
          JobStats jobStats = null;
          synchronized (mJobs) {
            jobStats = mJobs.poll();
          }
          while (jobStats != null) {
            Job job = jobStats.getJob();
            
            try {
              // get the job status
              long start = System.currentTimeMillis();
              JobStatus status = job.getStatus(); // cache the job status
              long end = System.currentTimeMillis();
              
              if (LOG.isDebugEnabled()) {
                LOG.debug("Status polling for job " + job.getJobID() + " took "
                          + (end-start) + "ms.");
              }
              
              // update the job progress
              jobStats.updateJobStatus(status);
              
              // if the job is complete, let others know
              if (status.isJobComplete()) {
                if (status.getState() == JobStatus.State.SUCCEEDED) {
                  onSuccess(job);
                } else {
                  onFailure(job);
                }
                synchronized (statistics) {
                  statistics.add(jobStats);
                }
              } else {
                // add the running job back and break
                synchronized (mJobs) {
                  if (!mJobs.offer(jobStats)) {
                    LOG.error("Lost job " + (null == job.getJobName()
                         ? "<unknown>" : job.getJobName())); // should never
                                                             // happen
                  }
                }
                break;
              }
            } catch (IOException e) {
              if (e.getCause() instanceof ClosedByInterruptException) {
                // Job doesn't throw InterruptedException, but RPC socket layer
                // is blocking and may throw a wrapped Exception if this thread
                // is interrupted. Since the lower level cleared the flag,
                // reset it here
                Thread.currentThread().interrupt();
              } else {
                LOG.warn("Lost job " + (null == job.getJobName()
                     ? "<unknown>" : job.getJobName()), e);
                synchronized (statistics) {
                  statistics.add(jobStats);
                }
              }
            }
            
            // get the next job
            synchronized (mJobs) {
              jobStats = mJobs.poll();
            }
          }
          
          // sleep for a while before checking again
          try {
            TimeUnit.MILLISECONDS.sleep(pollDelayMillis);
          } catch (InterruptedException e) {
            shutdown = true;
            continue;
          }
        } catch (Throwable e) {
          LOG.warn("Unexpected exception: ", e);
        }
      }
    }
  }

  /**
   * Start the internal, monitoring thread.
   */
  public void start() {
    for (int i = 0; i < numPollingThreads; ++i) {
      executor.execute(new MonitorThread(i));
    }
  }

  /**
   * Wait for the monitor to halt, assuming shutdown or abort have been
   * called. Note that, since submission may be sporatic, this will hang
   * if no form of shutdown has been requested.
   */
  public void join(long millis) throws InterruptedException {
    executor.awaitTermination(millis, TimeUnit.MILLISECONDS);
  }

  /**
   * Drain all submitted jobs to a queue and stop the monitoring thread.
   * Upstream submitter is assumed dead.
   */
  public void abort() {
    synchronized (mJobs) {
      graceful = false;
      shutdown = true;
    }
    executor.shutdown();
  }

  /**
   * When all monitored jobs have completed, stop the monitoring thread.
   * Upstream submitter is assumed dead.
   */
  public void shutdown() {
    synchronized (mJobs) {
      graceful = true;
      shutdown = true;
    }
    executor.shutdown();
  }
}


