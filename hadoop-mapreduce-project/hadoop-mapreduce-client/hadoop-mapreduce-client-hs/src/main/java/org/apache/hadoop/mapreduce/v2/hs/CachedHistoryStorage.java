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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Manages an in memory cache of parsed Job History files.
 */
public class CachedHistoryStorage extends AbstractService implements
    HistoryStorage {
  private static final Log LOG = LogFactory.getLog(CachedHistoryStorage.class);

  private LoadingCache<JobId, Job> loadedJobCache = null;
  private int loadedJobCacheSize;
  private int loadedTasksCacheSize;
  private boolean useLoadedTasksCache;

  private HistoryFileManager hsManager;

  @Override
  public void setHistoryFileManager(HistoryFileManager hsManager) {
    this.hsManager = hsManager;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    LOG.info("CachedHistoryStorage Init");

    createLoadedJobCache(conf);
  }

  @SuppressWarnings("serial")
  private void createLoadedJobCache(Configuration conf) {
    // Set property for old "loaded jobs" cache
    loadedJobCacheSize = conf.getInt(
        JHAdminConfig.MR_HISTORY_LOADED_JOB_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_LOADED_JOB_CACHE_SIZE);

    // Check property for new "loaded tasks" cache perform sanity checking
    useLoadedTasksCache = false;
    try {
      String taskSizeString = conf
          .get(JHAdminConfig.MR_HISTORY_LOADED_TASKS_CACHE_SIZE);
      if (taskSizeString != null) {
        loadedTasksCacheSize = Math.max(Integer.parseInt(taskSizeString), 1);
        useLoadedTasksCache = true;
      }
    } catch (NumberFormatException nfe) {
      LOG.error("The property " +
          JHAdminConfig.MR_HISTORY_LOADED_TASKS_CACHE_SIZE +
          " is not an integer value.  Please set it to a positive" +
          " integer value.");
    }

    CacheLoader<JobId, Job> loader;
    loader = new CacheLoader<JobId, Job>() {
      @Override
      public Job load(JobId key) throws Exception {
        return loadJob(key);
      }
    };

    if (!useLoadedTasksCache) {
      loadedJobCache = CacheBuilder.newBuilder()
          .maximumSize(loadedJobCacheSize)
          .initialCapacity(loadedJobCacheSize)
          .concurrencyLevel(1)
          .build(loader);
    } else {
      Weigher<JobId, Job> weightByTasks;
      weightByTasks = new Weigher<JobId, Job>() {
        /**
         * Method for calculating Job weight by total task count.  If
         * the total task count is greater than the size of the tasks
         * cache, then cap it at the cache size.  This allows the cache
         * to always hold one large job.
         * @param key JobId object
         * @param value Job object
         * @return Weight of the job as calculated by total task count
         */
        @Override
        public int weigh(JobId key, Job value) {
          int taskCount = Math.min(loadedTasksCacheSize,
              value.getTotalMaps() + value.getTotalReduces());
          return taskCount;
        }
      };
      // Keep concurrencyLevel at 1.  Otherwise, two problems:
      // 1) The largest job that can be initially loaded is
      //    cache size / 4.
      // 2) Unit tests are not deterministic.
      loadedJobCache = CacheBuilder.newBuilder()
          .maximumWeight(loadedTasksCacheSize)
          .weigher(weightByTasks)
          .concurrencyLevel(1)
          .build(loader);
    }
  }
  
  public void refreshLoadedJobCache() {
    if (getServiceState() == STATE.STARTED) {
      setConfig(createConf());
      createLoadedJobCache(getConfig());
    } else {
      LOG.warn("Failed to execute refreshLoadedJobCache: CachedHistoryStorage is not started");
    }
  }
  
  @VisibleForTesting
  Configuration createConf() {
    return new Configuration();
  }
  
  public CachedHistoryStorage() {
    super(CachedHistoryStorage.class.getName());
  }

  private static class HSFileRuntimeException extends RuntimeException {
    public HSFileRuntimeException(String message) {
      super(message);
    }
  }
  
  private Job loadJob(JobId jobId) throws RuntimeException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking for Job " + jobId);
    }
    HistoryFileInfo fileInfo;

    fileInfo = hsManager.getFileInfo(jobId);
    if (fileInfo == null) {
      throw new HSFileRuntimeException("Unable to find job " + jobId);
    } else if (fileInfo.isDeleted()) {
      throw new HSFileRuntimeException("Cannot load deleted job " + jobId);
    } else {
      return fileInfo.loadJob();
    }
  }

  @VisibleForTesting
  Cache<JobId, Job> getLoadedJobCache() {
    return loadedJobCache;
  }
  
  @Override
  public Job getFullJob(JobId jobId) {
    Job retVal = null;
    try {
      retVal = loadedJobCache.getUnchecked(jobId);
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof HSFileRuntimeException) {
        LOG.error(e.getCause().getMessage());
        return null;
      } else {
        throw new YarnRuntimeException(e.getCause());
      }
    }
    return retVal;
  }

  @Override
  public Map<JobId, Job> getAllPartialJobs() {
    LOG.debug("Called getAllPartialJobs()");
    SortedMap<JobId, Job> result = new TreeMap<JobId, Job>();
    try {
      for (HistoryFileInfo mi : hsManager.getAllFileInfo()) {
        if (mi != null) {
          JobId id = mi.getJobId();
          result.put(id, new PartialJob(mi.getJobIndexInfo(), id));
        }
      }
    } catch (IOException e) {
      LOG.warn("Error trying to scan for all FileInfos", e);
      throw new YarnRuntimeException(e);
    }
    return result;
  }

  @Override
  public JobsInfo getPartialJobs(Long offset, Long count, String user,
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
      JobState jobState) {
    return getPartialJobs(getAllPartialJobs().values(), offset, count, user,
        queue, sBegin, sEnd, fBegin, fEnd, jobState);
  }

  public static JobsInfo getPartialJobs(Collection<Job> jobs, Long offset,
      Long count, String user, String queue, Long sBegin, Long sEnd,
      Long fBegin, Long fEnd, JobState jobState) {
    JobsInfo allJobs = new JobsInfo();

    if (sBegin == null || sBegin < 0)
      sBegin = 0l;
    if (sEnd == null)
      sEnd = Long.MAX_VALUE;
    if (fBegin == null || fBegin < 0)
      fBegin = 0l;
    if (fEnd == null)
      fEnd = Long.MAX_VALUE;
    if (offset == null || offset < 0)
      offset = 0l;
    if (count == null)
      count = Long.MAX_VALUE;

    if (offset > jobs.size()) {
      return allJobs;
    }

    long at = 0;
    long end = offset + count - 1;
    if (end < 0) { // due to overflow
      end = Long.MAX_VALUE;
    }

    for (Job job : jobs) {
      if (at > end) {
        break;
      }

      // can't really validate queue is a valid one since queues could change
      if (queue != null && !queue.isEmpty()) {
        if (!job.getQueueName().equals(queue)) {
          continue;
        }
      }

      if (user != null && !user.isEmpty()) {
        if (!job.getUserName().equals(user)) {
          continue;
        }
      }

      JobReport report = job.getReport();

      if (report.getStartTime() < sBegin || report.getStartTime() > sEnd) {
        continue;
      }
      if (report.getFinishTime() < fBegin || report.getFinishTime() > fEnd) {
        continue;
      }
      if (jobState != null && jobState != report.getJobState()) {
        continue;
      }

      at++;
      if ((at - 1) < offset) {
        continue;
      }

      JobInfo jobInfo = new JobInfo(job);

      allJobs.add(jobInfo);
    }
    return allJobs;
  }

  @VisibleForTesting
  public boolean getUseLoadedTasksCache() {
    return useLoadedTasksCache;
  }

  @VisibleForTesting
  public int getLoadedTasksCacheSize() {
    return loadedTasksCacheSize;
  }
}
