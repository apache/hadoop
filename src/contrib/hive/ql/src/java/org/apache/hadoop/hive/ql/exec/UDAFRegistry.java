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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.TypeInfo;
import org.apache.hadoop.hive.ql.udf.*;

public class UDAFRegistry {

  private static Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.exec.UDAFRegistry");

  /**
   * The mapping from aggregation function names to aggregation classes.
   */
  static HashMap<String, Class<? extends UDAF>> mUDAFs;
  static {
    mUDAFs = new HashMap<String, Class<? extends UDAF>>();
    registerUDAF("sum", UDAFSum.class);
    registerUDAF("count", UDAFCount.class);
    registerUDAF("max", UDAFMax.class);
    registerUDAF("min", UDAFMin.class);
    registerUDAF("avg", UDAFAvg.class);
  }

  public static void registerUDAF(String functionName, Class<? extends UDAF> UDAFClass) {
    if (UDAF.class.isAssignableFrom(UDAFClass)) { 
      mUDAFs.put(functionName.toLowerCase(), UDAFClass);
    } else {
      throw new RuntimeException("Registering UDAF Class " + UDAFClass + " which does not extends " + UDAF.class);
    }
    mUDAFs.put(functionName.toLowerCase(), UDAFClass);
  }
  
  public static Class<? extends UDAF> getUDAF(String functionName) {
    LOG.debug("Looking up UDAF: " + functionName);
    Class<? extends UDAF> result = mUDAFs.get(functionName.toLowerCase());
    return result;
  }

  public static Method getUDAFMethod(String name, List<Class<?>> argumentClasses) {
    Class<? extends UDAF> udaf = getUDAF(name);
    if (udaf == null) return null;
    return UDFRegistry.getMethodInternal(udaf, "aggregate", false, argumentClasses);
  }

  public static Method getUDAFMethod(String name, Class<?> ... argumentClasses) {
    return getUDAFMethod(name, Arrays.asList(argumentClasses));
  }
  
  
}
