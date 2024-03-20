/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.common;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HdfsCompatReport {
  private final String uri;
  private final HdfsCompatSuite suite;
  private final ConcurrentLinkedQueue<String> passed =
      new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<String> failed =
      new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<String> skipped =
      new ConcurrentLinkedQueue<>();

  public HdfsCompatReport() {
    this(null, null);
  }

  public HdfsCompatReport(String uri, HdfsCompatSuite suite) {
    this.uri = uri;
    this.suite = suite;
  }

  public void addPassedCase(Collection<String> cases) {
    passed.addAll(cases);
  }

  public void addFailedCase(Collection<String> cases) {
    failed.addAll(cases);
  }

  public void addSkippedCase(Collection<String> cases) {
    skipped.addAll(cases);
  }

  public void merge(HdfsCompatReport other) {
    this.passed.addAll(other.passed);
    this.failed.addAll(other.failed);
    this.skipped.addAll(other.skipped);
  }

  public Collection<String> getPassedCase() {
    return passed;
  }

  public Collection<String> getFailedCase() {
    return failed;
  }

  public Collection<String> getSkippedCase() {
    return skipped;
  }

  public String getUri() {
    return this.uri;
  }

  public HdfsCompatSuite getSuite() {
    return this.suite;
  }
}