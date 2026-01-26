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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.regex.Pattern;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * An appender that matches logged messages against the given
 * regular expression.
 */
public class PatternMatchingAppender extends AppenderSkeleton {
  private final Pattern pattern;
  private volatile boolean matched;

  public PatternMatchingAppender() {
    this.pattern = Pattern.compile("^.*FakeMetric.*$");
    this.matched = false;
  }

  public boolean isMatched() {
    return matched;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (pattern.matcher(event.getMessage().toString()).matches()) {
      matched = true;
    }
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
