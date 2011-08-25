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

import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * JSON helpers
 */
public class Jsons {
  public static final String _SEP = "\",\"";

  public static PrintWriter appendProgressBar(PrintWriter out, String pct) {
    return out.append("<br title='").append(pct).append("'>").
        append("<div class='").append(C_PROGRESSBAR).
        append("' title='").append(pct).append('%').
        append("'><div class='").append(C_PROGRESSBAR_VALUE).
        append("' style='width: ").append(pct).
        append("%'>").append("<\\/div><\\/div>");
  }

  public static PrintWriter appendProgressBar(PrintWriter out,
                                                  float progress) {
    return appendProgressBar(out, String.format("%.1f", progress * 100));
  }

  public static PrintWriter appendSortable(PrintWriter out, Object value) {
    return out.append("<br title='").append(String.valueOf(value)).append("'>");
  }

  public static PrintWriter appendLink(PrintWriter out, Object anchor,
                                       String prefix, String... parts) {
    String anchorText = String.valueOf(anchor);
    return out.append("<a href='").append(anchor == null ? "#" :
      ujoin(prefix, parts)).append("'>").append(anchorText).append("<\\/a>");
  }
}
