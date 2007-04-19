/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.log4j.Level;
/**
 * Retrieve environment variables that control debugging and logging environment
 */
public class Environment {
  public static boolean debugging = false;
  public static Level logLevel = Level.INFO;
  
  private Environment() {};                          // Not instantiable
  
  public static void getenv() {
    String value = null;
    
    value = System.getenv("DEBUGGING");
    if (value != null && value.equalsIgnoreCase("TRUE")) {
      debugging = true;
    }
    
    value = System.getenv("LOGGING_LEVEL");
    if (value != null && value.length() != 0) {
      if (value.equalsIgnoreCase("ALL")) {
        logLevel = Level.ALL;
      } else if (value.equalsIgnoreCase("DEBUG")) {
        logLevel = Level.DEBUG;
      } else if (value.equalsIgnoreCase("ERROR")) {
        logLevel = Level.ERROR;
      } else if (value.equalsIgnoreCase("FATAL")) {
        logLevel = Level.FATAL;
      } else if (value.equalsIgnoreCase("INFO")) {
        logLevel = Level.INFO;
      } else if (value.equalsIgnoreCase("OFF")) {
        logLevel = Level.OFF;
      } else if (value.equalsIgnoreCase("TRACE")) {
        logLevel = Level.TRACE;
      } else if (value.equalsIgnoreCase("WARN")) {
        logLevel = Level.WARN;
      }
    }
  }
  
}
