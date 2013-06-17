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

package org.apache.hadoop.yarn.webapp.view;

import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.View;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class TextView extends View {

  private final String contentType;

  protected TextView(ViewContext ctx, String contentType) {
    super(ctx);
    this.contentType = contentType;
  }

  @Override public PrintWriter writer() {
    response().setContentType(contentType);
    return super.writer();
  }

  /**
   * Print strings as is (no newline, a la php echo).
   * @param args the strings to print
   */
  public void echo(Object... args) {
    PrintWriter out = writer();
    for (Object s : args) {
      out.print(s);
    }
  }

  /**
   * Print strings as a line (new line appended at the end, a la C/Tcl puts).
   * @param args the strings to print
   */
  public void puts(Object... args) {
    echo(args);
    writer().println();
  }
}
