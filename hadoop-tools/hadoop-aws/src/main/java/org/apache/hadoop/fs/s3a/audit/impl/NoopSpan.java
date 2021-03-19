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

package org.apache.hadoop.fs.s3a.audit.impl;

/**
 * Simple no-op span.
 */
public final class NoopSpan extends AbstractAuditSpanImpl {

  private final String name;

  private final String path1;

  private final String path2;

  /**
   * Static public instance.
   */
  public static final NoopSpan INSTANCE = new NoopSpan();

  /**
   * Create a no-op span.
   * @param name name
   * @param path1 path
   * @param path2 path 2
   */
  public NoopSpan(final String name, final String path1, final String path2) {
    this.name = name;
    this.path1 = path1;
    this.path2 = path2;
  }

  NoopSpan() {
    this("no-op", null, null);
  }

  @Override
  public void deactivate() {
    // no-op
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NoopSpan{");
    sb.append("name='").append(name).append('\'');
    sb.append(", path1='").append(path1).append('\'');
    sb.append(", path2='").append(path2).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
