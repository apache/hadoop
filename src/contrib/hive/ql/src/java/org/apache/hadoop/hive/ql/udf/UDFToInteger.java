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


public class UDFToInteger implements UDF {

  private static Log LOG = LogFactory.getLog(UDFToInteger.class.getName());

  public UDFToInteger() {
  }

  public Integer evaluate(Boolean i)  {
    if (i == null) {
      return null;
    } else {
      return i.booleanValue() ? 1 : 0;
    }
  }
  
  public Integer evaluate(Byte i)  {
    if (i == null) {
      return null;
    } else {
      return Integer.valueOf(i.intValue());
    }
  }
  
  public Integer evaluate(Long i)  {
    if (i == null) {
      return null;
    } else {
      return Integer.valueOf(i.intValue());
    }
  }
  
  public Integer evaluate(Float i)  {
    if (i == null) {
      return null;
    } else {
      return Integer.valueOf(i.intValue());
    }
  }
  
  public Integer evaluate(Double i)  {
    if (i == null) {
      return null;
    } else {
      return Integer.valueOf(i.intValue());
    }
  }
  
  public Integer evaluate(String i)  {
    if (i == null) {
      return null;
    } else {
      try {
        return Integer.valueOf(i);
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // return Integer.valueOf(0);
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }
  
}
