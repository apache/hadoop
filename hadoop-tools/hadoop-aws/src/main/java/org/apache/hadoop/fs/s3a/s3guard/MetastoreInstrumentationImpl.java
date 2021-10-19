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
 * A no-op implementation of {@link MetastoreInstrumentation}
 * which allows metastores to always return an instance
 * when requested.
 */
public class MetastoreInstrumentationImpl implements MetastoreInstrumentation {

  @Override
  public void initialized() {

  }

  @Override
  public void storeClosed() {

  }

  @Override
  public void throttled() {

  }

  @Override
  public void retrying() {

  }

  @Override
  public void recordsDeleted(final int count) {

  }

  @Override
  public void recordsRead(final int count) {

  }

  @Override
  public void recordsWritten(final int count) {

  }

  @Override
  public void directoryMarkedAuthoritative() {

  }

  @Override
  public void entryAdded(final long durationNanos) {

  }
}
