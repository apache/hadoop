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

package org.apache.hadoop.hive.serde;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;

public class ExpressionUtils {

  private static Pattern metachars = Pattern.compile(".*[\\.#\\[\\]].*");
  public static boolean isComplexExpression(String exp) {
    Matcher m = metachars.matcher(exp);
    return(m.matches());
  }

  /** Decompose a complex expression into 2 parts: top-level field, and the rest of the expression.
   * 
   * @param exp  the expression to decompose
   * @return     a List of size 2, containing the top-level field, and the rest of the expression, 
   *             or a List of size 1, if exp is a simple expression.
   */
  public static List<String> decomposeComplexExpression(String exp) {
    ArrayList<String> result = new ArrayList<String>();
    int posDot = exp.indexOf('.');
    int posSquare = exp.indexOf('[');
    int pos = posDot;
    if (posDot == -1 || (posSquare != -1 && posSquare < posDot)) {
      pos = posSquare;
    }
    if (pos == -1) {
      result.add(exp);
    } else {
      String topLevelField = exp.substring(0,pos);
      String suffixField = exp.substring(pos);
      result.add(topLevelField);
      result.add(suffixField);
    }
    return result;
  }
  
}
