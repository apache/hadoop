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

package org.apache.hadoop.fs.s3a.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A duration with logging of final state at info in the {@code close()} call.
 * This allows it to be used in a try-with-resources clause, and have the
 * duration automatically logged.
 */
@InterfaceAudience.Private
public class DurationInfo extends Duration
    implements AutoCloseable {
  private final String text;
  private static final Logger LOG = LoggerFactory.getLogger(DurationInfo.class);

  /**
   * Create the duration text from a {@code String.format()} code call.
   * @param format format string
   * @param args list of arguments
   */
  public DurationInfo(String format, Object... args) {
    this.text = String.format(format, args);
    LOG.info("Starting: {}", text);
  }

  @Override
  public String toString() {
    return text + ": duration " + super.toString();
  }

  @Override
  public void close() {
    finished();
    LOG.info(this.toString());
  }
}
