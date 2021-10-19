/*
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

package org.apache.hadoop.fs.s3a.s3guard;

/**
 * This interface is defined for handling TTL expiry of metadata in S3Guard.
 *
 * TTL can be tested by implementing this interface and setting is as
 * {@code S3Guard.ttlTimeProvider}. By doing this, getNow() can return any
 * value preferred and flaky tests could be avoided. By default getNow()
 * returns the EPOCH in runtime.
 *
 * Time is measured in milliseconds,
 */
public interface ITtlTimeProvider {

  /**
   * The current time in milliseconds.
   * Assuming this calls System.currentTimeMillis(), this is a native iO call
   * and so should be invoked sparingly (i.e. evaluate before any loop, rather
   * than inside).
   * @return the current time.
   */
  long getNow();

  /**
   * The TTL of the metadata.
   * @return time in millis after which metadata is considered out of date.
   */
  long getMetadataTtl();
}
