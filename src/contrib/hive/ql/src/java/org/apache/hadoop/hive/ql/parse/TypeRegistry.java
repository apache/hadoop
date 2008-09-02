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

package org.apache.hadoop.hive.ql.parse;

import java.util.HashMap;
import java.lang.Number;
import java.lang.Boolean;
import java.lang.String;
import java.util.Date;
import java.lang.Void;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that stores the type mappings for all builtin functions, udfs, udafs
 * and builtin operators. The mappings are (name, list of paramerter types) -> return type
 *
 **/

public class TypeRegistry {
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.TypeRegistry");

  private static HashMap<InputSignature, FunctionInfo> typeMap;
  static {
    typeMap = new HashMap<InputSignature, FunctionInfo>();
    typeMap.put(new InputSignature("+", Number.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", Void.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", Void.class, Void.class), 
        new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", String.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", Void.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("+", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("-", Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Number.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Void.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Void.class, Void.class), 
        new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", String.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Void.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", String.class, Void.class), 
        new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Number.class, Void.class), 
        new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("-", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("/", Number.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", Void.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", Void.class, Void.class), 
        new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", String.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", Void.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("/", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("*", Number.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", Void.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", Void.class, Void.class), 
        new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", String.class, Void.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", Void.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("*", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("%", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("%", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("%", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("%", Number.class, String.class), 
                new FunctionInfo(Number.class, null));


    typeMap.put(new InputSignature("&", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("&", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("&", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("&", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("~", Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("~", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("~", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("~", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("|", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("|", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("|", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("|", Number.class, String.class), 
                new FunctionInfo(Number.class, null));

    typeMap.put(new InputSignature("^", Number.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("^", String.class, String.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("^", String.class, Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("^", Number.class, String.class), 
                new FunctionInfo(Number.class, null));


    typeMap.put(new InputSignature("=", Number.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", String.class, String.class), 
                new FunctionInfo(Boolean.class, "str_eq"));
    typeMap.put(new InputSignature("=", Number.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", String.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", Date.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", Date.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", String.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("=", Void.class, Void.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature("<>", Number.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<>", String.class, String.class), 
                new FunctionInfo(Boolean.class, "str_ne"));
    typeMap.put(new InputSignature("<>", Number.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<>", String.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<>", Date.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<>", Date.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<>", String.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<>", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature("<=", Number.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<=", String.class, String.class), 
                new FunctionInfo(Boolean.class, "str_le"));
    typeMap.put(new InputSignature("<=", Number.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<=", String.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<=", Date.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<=", Date.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<=", String.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<=", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature("<", Number.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<", String.class, String.class), 
                new FunctionInfo(Boolean.class, "str_lt"));
    typeMap.put(new InputSignature("<", Number.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<", String.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<", Date.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<", Date.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<", String.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("<", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature(">=", Number.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">=", String.class, String.class), 
                new FunctionInfo(Boolean.class, "str_ge"));
    typeMap.put(new InputSignature(">=", Number.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">=", String.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">=", Date.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">=", Date.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">=", String.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">=", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature(">", Number.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">", String.class, String.class), 
                new FunctionInfo(Boolean.class, "str_gt"));
    typeMap.put(new InputSignature(">", Number.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">", String.class, Number.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">", Date.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">", Date.class, String.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">", String.class, Date.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature(">", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature("AND", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("OR", Boolean.class, Boolean.class), 
                new FunctionInfo(Boolean.class, null));
    typeMap.put(new InputSignature("NOT", Boolean.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature("sum", Number.class), 
                new FunctionInfo(Number.class, null));
    typeMap.put(new InputSignature("count", Object.class), 
                new FunctionInfo(Boolean.class, null));

    typeMap.put(new InputSignature("concat", String.class, String.class), 
                new FunctionInfo(String.class, null));
    typeMap.put(new InputSignature("substr", String.class, Number.class), 
                new FunctionInfo(String.class, null));
    typeMap.put(new InputSignature("substr", String.class, Number.class, Number.class), 
                new FunctionInfo(String.class, null));
    typeMap.put(new InputSignature("NULL", Void.class), 
                new FunctionInfo(Void.class, null));
    typeMap.put(new InputSignature("NULL", String.class), 
                new FunctionInfo(Void.class, null));
  }

  public static FunctionInfo getTypeInfo(InputSignature sgn) {
    LOG.info("Looking up: " + sgn.toString());
    return typeMap.get(sgn);
  }

  public static boolean isValidFunction(String functionName) {
    for(InputSignature inp: typeMap.keySet()) {
      if (inp.getName().equalsIgnoreCase(functionName)) {
        return true;
      }
    }
    return false;
  }

}
