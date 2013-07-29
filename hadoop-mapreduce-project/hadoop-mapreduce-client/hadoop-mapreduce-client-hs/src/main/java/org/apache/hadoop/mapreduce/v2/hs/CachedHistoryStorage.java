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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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

  private Map<JobId, Job> loadedJobCache = null;
  // The number of loaded jobs.
  private int loadedJobCacheSize;

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
    loadedJobCacheSize = conf.getInt(
        JHAdminConfig.MR_HISTORY_LOADED_JOB_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_LOADED_JOB_CACHE_SIZE);

    loadedJobCache = Collections.synchronizedMap(new LinkedHashMap<JobId, Job>(
        loadedJobCacheSize + 1, 0.75f, true) {
      @Override
      public boolean removeEldestEntry(final Map.Entry<JobId, Job> eldest) {
        return super.size() > loadedJobCacheSize;
      }
    });
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
  
  private Job loadJob(HistoryFileInfo fileInfo) {
    try {
      Job job = fileInfo.loadJob();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding " + job.getID() + " to loaded job cache");
      }
      // We can clobber results here, but that should be OK, because it only
      // means that we may have two identical copies of the same job floating
      // around for a while.
      loadedJobCache.put(job.getID(), job);
      return job;
    } catch (IOException e) {
      throw new YarnRuntimeException(
          "Could not find/load job: " + fileInfo.getJobId(), e);
    }
  }

  @VisibleForTesting
  Map<JobId, Job> getLoadedJobCache() {
    return loadedJobCache;
  }
  
  @Override
  public Job getFullJob(JobId jobId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking for Job " + jobId);
    }
    try {
      HistoryFileInfo fileInfo = hsManager.getFileInfo(jobId);
      Job result = null;
      if (fileInfo != null) {
        result = loadedJobCache.get(jobId);
        if (result == null) {
          result = loadJob(fileInfo);
        } else if(fileInfo.isDeleted()) {
          loadedJobCache.remove(jobId);
          result = null;
        }
      } else {
        loadedJobCache.remove(jobId);
      }
      return result;
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
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
}
