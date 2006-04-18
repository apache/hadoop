/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.hadoop.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * General string utils
 * @author Owen O'Malley
 */
public class StringUtils {

  /**
   * Make a string representation of the exception.
   * @param e The exception to stringify
   * @return A string with exception name and call stack.
   */
  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

}
