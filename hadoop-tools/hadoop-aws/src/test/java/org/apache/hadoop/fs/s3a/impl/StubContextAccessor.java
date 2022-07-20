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

package org.apache.hadoop.fs.s3a.impl;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AuditTestSupport;
import org.apache.hadoop.fs.store.audit.AuditSpan;

/**
 * A Stub context acccessor for test.
 */
public final class StubContextAccessor
    implements ContextAccessors {

  private final String bucket;

  /**
   * Construct.
   * @param bucket bucket to use when qualifying keys.]=
   */
  public StubContextAccessor(String bucket) {
    this.bucket = bucket;
  }

  @Override
  public Path keyToPath(final String key) {
    return new Path("s3a://" + bucket + "/" + key);
  }

  @Override
  public String pathToKey(final Path path) {
    return null;
  }

  @Override
  public File createTempFile(final String prefix, final long size)
      throws IOException {
    throw new UnsupportedOperationException("unsppported");
  }

  @Override
  public String getBucketLocation() throws IOException {
    return null;
  }

  @Override
  public Path makeQualified(final Path path) {
    return path;
  }

  @Override
  public AuditSpan getActiveAuditSpan() {
    return AuditTestSupport.NOOP_SPAN;
  }

  @Override
  public RequestFactory getRequestFactory() {
    return MockS3AFileSystem.REQUEST_FACTORY;
  }

}
