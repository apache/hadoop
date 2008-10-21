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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class UDFRegExpReplace implements UDF {

  private String lastRegex = null;
  private Pattern p = null;

  public UDFRegExpReplace() {
  }

  public String evaluate(String s, String regex, String replacement) {
    if (s == null || regex == null || replacement == null) {
      return null;
    }
    if (!regex.equals(lastRegex)) {
      lastRegex = regex;
      p = Pattern.compile(regex);
    }
    Matcher m = p.matcher(s);
    
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, replacement);
    }
    m.appendTail(sb);
    return sb.toString();    
  }

}
