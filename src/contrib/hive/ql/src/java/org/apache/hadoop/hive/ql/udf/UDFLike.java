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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class UDFLike implements UDF {

  private static Log LOG = LogFactory.getLog(UDFLike.class.getName());
  private String lastLikePattern = null;
  private Pattern p = null;

  public UDFLike() {
  }

  public static String likePatternToRegExp(String likePattern) {
    StringBuilder sb = new StringBuilder();
    for(int i=0; i<likePattern.length(); i++) {
      // Make a special case for "\\_" and "\\%"
      char n = likePattern.charAt(i);
      if (n == '\\' && i+1 < likePattern.length() 
          && (likePattern.charAt(i+1) == '_' || likePattern.charAt(i+1) == '%')) {
        sb.append(likePattern.charAt(i+1));
        i++;
        continue;
      }

      if (n == '_') {
        sb.append(".");
      } else if (n == '%') {
        sb.append(".*");
      } else {
        if ("\\[](){}.*^$".indexOf(n) != -1) {
          sb.append('\\');
        }
        sb.append(n);
      }
    }
    return sb.toString();
  }
  
  public Boolean evaluate(String s, String likePattern) {
    if (s == null || likePattern == null) {
      return null;
    }
    if (!likePattern.equals(lastLikePattern)) {
      lastLikePattern = likePattern;
      p = Pattern.compile(likePatternToRegExp(likePattern));
    }
    Matcher m = p.matcher(s);
    return Boolean.valueOf(m.matches());    
  }
  
}
