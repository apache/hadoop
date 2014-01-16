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

package org.apache.hadoop.yarn;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.log4j.RollingFileAppender;

import java.io.File;
import java.io.Flushable;

/**
 * A simple log4j-appender for container's logs.
 *
 */
@Public
@Unstable
public class ContainerRollingLogAppender extends RollingFileAppender
  implements Flushable {
  private String containerLogDir;

  @Override
  public void activateOptions() {
    synchronized (this) {
      setFile(new File(this.containerLogDir, "syslog").toString());
      setAppend(true);
      super.activateOptions();
    }
  }

  @Override
  public void flush() {
    if (qw != null) {
      qw.flush();
    }
  }

  /**
   * Getter/Setter methods for log4j.
   */

  public String getContainerLogDir() {
    return this.containerLogDir;
  }

  public void setContainerLogDir(String containerLogDir) {
    this.containerLogDir = containerLogDir;
  }
}
