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

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TimelineVersionWatcher extends TestWatcher {
  static final float DEFAULT_TIMELINE_VERSION = 1.0f;
  private TimelineVersion version;

  @Override
  protected void starting(Description description) {
    version = description.getAnnotation(TimelineVersion.class);
  }

  /**
   * @return the version number of timeline server for the current test (using
   * timeline server v1.0 by default)
   */
  public float getTimelineVersion() {
    if(version == null) {
      return DEFAULT_TIMELINE_VERSION;
    }
    return version.value();
  }
}
