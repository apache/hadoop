 /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */

 package org.apache.hadoop.fs.impl.prefetch;

 import java.time.Duration;

 import org.apache.hadoop.fs.statistics.DurationTracker;
 import org.apache.hadoop.fs.statistics.IOStatisticsSource;

 import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.stubDurationTracker;

 public interface PrefetchingStatistics extends IOStatisticsSource {

   /**
    * A prefetch operation has started.
    * @return duration tracker
    */
   DurationTracker prefetchOperationStarted();

   /**
    * A block fetch operation has started.
    * @return duration tracker
    */
   default DurationTracker blockFetchOperationStarted() {
     return stubDurationTracker();
   }

   /**
    * A block has been saved to the file cache.
    */
   void blockAddedToFileCache();

   /**
    * A block has been removed from the file cache.
    */
   void blockRemovedFromFileCache();

   /**
    * A block has been evicted from the file cache.
    */
   void blockEvictedFromFileCache();

   /**
    * A prefetch operation has completed.
    */
   void prefetchOperationCompleted();

   /**
    * A fetch/prefetch operation has completed.
    * @param prefetch true if this was a prefetch
    * @param bytesFetched number of bytes fetch
    */
   void fetchOperationCompleted(boolean prefetch, long bytesFetched);

   /**
    * An executor has been acquired, either for prefetching or caching.
    * @param timeInQueue time taken to acquire an executor.
    */
   void executorAcquired(Duration timeInQueue);

   /**
    * A new buffer has been added to the buffer pool.
    * @param size size of the new buffer
    */
   void memoryAllocated(int size);

   /**
    * Previously allocated memory has been freed.
    * @param size size of memory freed.
    */
   void memoryFreed(int size);

   /**
    * Publish the prefetching state through a gauge.
    * @param prefetchEnabled is prefetching enabled?
    * @param blocks number of blocks to fetch
    * @param blocksize block size.
    */
   default void setPrefetchState(boolean prefetchEnabled, int blocks, int blocksize) {

   }

   /**
    * Publish the disk caching state through a gauge.
    * @param cacheEnabled will blocks be cached?
    */
   default void setPrefetchDiskCachingState(boolean cacheEnabled) {

   }

   /**
    * Bytes read from buffer (rather than via any direct http request).
    * @param bytes number of bytes read.
    */
   default void bytesReadFromBuffer(long bytes) {

   }
 }
