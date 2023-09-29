/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Value class that stores user settings and can render data in XML format,
 * see {@link #render()}.
 */
public class UserSettings {
  private final String username;
  private final Integer maxRunningApps;

  UserSettings(Builder builder) {
    this.username = builder.username;
    this.maxRunningApps = builder.maxRunningApps;
  }

  public String render() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    addStartTag(pw);
    AllocationFileWriter.addIfPresent(pw, "maxRunningApps", maxRunningApps);
    addEndTag(pw);
    pw.close();

    return sw.toString();
  }

  private void addStartTag(PrintWriter pw) {
    pw.println("<user name=\"" + username + "\">");
  }

  private void addEndTag(PrintWriter pw) {
    pw.println("</user>");
  }

  /**
   * Builder class for {@link UserSettings}
   */
  public static class Builder {
    private final String username;
    private Integer maxRunningApps;

    public Builder(String username) {
      this.username = username;
    }

    public Builder maxRunningApps(int value) {
      this.maxRunningApps = value;
      return this;
    }

    public UserSettings build() {
      return new UserSettings(this);
    }
  }
}
