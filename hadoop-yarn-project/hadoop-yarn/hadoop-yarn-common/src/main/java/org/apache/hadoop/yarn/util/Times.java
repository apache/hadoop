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

package org.apache.hadoop.yarn.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class Times {
  static final ThreadLocal<SimpleDateFormat> dateFormat =
      new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
        }
      };

  public static long elapsed(long started, long finished) {
    return Times.elapsed(started, finished, true);
  }

  public static long elapsed(long started, long finished, boolean isRunning) {
    if (finished > 0) {
      return finished - started;
    }
    if (isRunning) {
      return started > 0 ? System.currentTimeMillis() - started : 0;
    } else {
      return -1;
    }
  }

  public static String format(long ts) {
    return ts > 0 ? String.valueOf(dateFormat.get().format(new Date(ts)))
                  : "N/A";
  }
}
