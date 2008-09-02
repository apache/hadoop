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


public class UDFOPLessThan extends UDFBaseCompare {

  private static Log LOG = LogFactory.getLog(UDFOPLessThan.class.getName());

  public UDFOPLessThan() {
  }

  public Boolean evaluate(String a, String b)  {

    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.compareTo(b) < 0);
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }


  public Boolean evaluate(Byte a, Byte b)  {
    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.byteValue() < b.byteValue());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }

  public Boolean evaluate(Integer a, Integer b)  {
    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.intValue() < b.intValue());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }
  
  public Boolean evaluate(Long a, Long b)  {
    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.longValue() < b.longValue());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }
  
  public Boolean evaluate(Float a, Float b)  {
    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.floatValue() < b.floatValue());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }  

  public Boolean evaluate(Double a, Double b)  {
    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.doubleValue() < b.doubleValue());
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }  

  public Boolean evaluate(Date a, Date b)  {
    Boolean r = null;
    if ((a == null) || (b == null)) {
      r = null;
    } else {
      r = Boolean.valueOf(a.compareTo(b) < 0);
    }
    // LOG.info("evaluate(" + a + "," + b + ")=" + r);
    return r;
  }
  
}
