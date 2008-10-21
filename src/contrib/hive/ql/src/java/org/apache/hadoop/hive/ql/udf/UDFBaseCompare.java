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

import java.sql.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;


public abstract class UDFBaseCompare implements UDF {

  private static Log LOG = LogFactory.getLog(UDFBaseCompare.class.getName());

  public UDFBaseCompare() {
  }

  public abstract Boolean evaluate(Double a, Double b);
  
  /** If one of the argument is a String and the other is a Number, convert
   *  String to double and the Number to double, and then compare.
   */
  public Boolean evaluate(String a, Number b)  {
    Double aDouble = null;
    try {
      aDouble = Double.valueOf(a);
    } catch (Exception e){
      // do nothing: aDouble will be null.
    }
    return evaluate(aDouble, new Double(b.doubleValue()));
  }

  /** If one of the argument is a String and the other is a Number, convert
   *  String to double and the Number to double, and then compare.
   */
  public Boolean evaluate(Number a, String b)  {
    Double bDouble = null;
    try {
      bDouble = Double.valueOf(b);
    } catch (Exception e){
      // do nothing: bDouble will be null.
    }
    return evaluate(new Double(a.doubleValue()), bDouble);
  }
  
}
