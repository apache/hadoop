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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.junit.Test;

public class TestCleanerTask {
  private static final String ROOT =
      YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT;
  private static final long SLEEP_TIME =
      YarnConfiguration.DEFAULT_SCM_CLEANER_RESOURCE_SLEEP_MS;
  private static final int NESTED_LEVEL =
      YarnConfiguration.DEFAULT_SHARED_CACHE_NESTED_LEVEL;

  @Test
  public void testNonExistentRoot() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    CleanerMetrics metrics = mock(CleanerMetrics.class);
    SCMStore store = mock(SCMStore.class);

    CleanerTask task =
        createSpiedTask(fs, store, metrics, new ReentrantLock());
    // the shared cache root does not exist
    when(fs.exists(task.getRootPath())).thenReturn(false);

    task.run();

    // process() should not be called
    verify(task, never()).process();
  }

  @Test
  public void testProcessFreshResource() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    CleanerMetrics metrics = mock(CleanerMetrics.class);
    SCMStore store = mock(SCMStore.class);

    CleanerTask task =
        createSpiedTask(fs, store, metrics, new ReentrantLock());

    // mock a resource that is not evictable
    when(store.isResourceEvictable(isA(String.class), isA(FileStatus.class)))
        .thenReturn(false);
    FileStatus status = mock(FileStatus.class);
    when(status.getPath()).thenReturn(new Path(ROOT + "/a/b/c/abc"));

    // process the resource
    task.processSingleResource(status);

    // the directory should not be renamed
    verify(fs, never()).rename(eq(status.getPath()), isA(Path.class));
    // metrics should record a processed file (but not delete)
    verify(metrics).reportAFileProcess();
    verify(metrics, never()).reportAFileDelete();
  }

  @Test
  public void testProcessEvictableResource() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    CleanerMetrics metrics = mock(CleanerMetrics.class);
    SCMStore store = mock(SCMStore.class);

    CleanerTask task =
        createSpiedTask(fs, store, metrics, new ReentrantLock());

    // mock an evictable resource
    when(store.isResourceEvictable(isA(String.class), isA(FileStatus.class)))
        .thenReturn(true);
    FileStatus status = mock(FileStatus.class);
    when(status.getPath()).thenReturn(new Path(ROOT + "/a/b/c/abc"));
    when(store.removeResource(isA(String.class))).thenReturn(true);
    // rename succeeds
    when(fs.rename(isA(Path.class), isA(Path.class))).thenReturn(true);
    // delete returns true
    when(fs.delete(isA(Path.class), anyBoolean())).thenReturn(true);

    // process the resource
    task.processSingleResource(status);

    // the directory should be renamed
    verify(fs).rename(eq(status.getPath()), isA(Path.class));
    // metrics should record a deleted file
    verify(metrics).reportAFileDelete();
    verify(metrics, never()).reportAFileProcess();
  }

  private CleanerTask createSpiedTask(FileSystem fs, SCMStore store,
      CleanerMetrics metrics, Lock isCleanerRunning) {
    return spy(new CleanerTask(ROOT, SLEEP_TIME, NESTED_LEVEL, fs, store,
        metrics, isCleanerRunning));
  }

  @Test
  public void testResourceIsInUseHasAnActiveApp() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    CleanerMetrics metrics = mock(CleanerMetrics.class);
    SCMStore store = mock(SCMStore.class);

    FileStatus resource = mock(FileStatus.class);
    when(resource.getPath()).thenReturn(new Path(ROOT + "/a/b/c/abc"));
    // resource is stale
    when(store.isResourceEvictable(isA(String.class), isA(FileStatus.class)))
        .thenReturn(true);
    // but still has appIds
    when(store.removeResource(isA(String.class))).thenReturn(false);

    CleanerTask task =
        createSpiedTask(fs, store, metrics, new ReentrantLock());

    // process the resource
    task.processSingleResource(resource);

    // metrics should record a processed file (but not delete)
    verify(metrics).reportAFileProcess();
    verify(metrics, never()).reportAFileDelete();
  }
}
