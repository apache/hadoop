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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.Void;

import org.apache.hadoop.hive.ql.parse.TypeInfo;
import org.apache.hadoop.hive.ql.udf.*;

public class UDFRegistry {

  private static Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.exec.UDFRegistry");

  /**
   * The mapping from expression function names to expression classes.
   */
  static HashMap<String, Class<? extends UDF>> mUDFs;
  static {
    mUDFs = new HashMap<String, Class<? extends UDF>>();
    registerUDF("default_sample_hashfn", UDFDefaultSampleHashFn.class);
    registerUDF("concat", UDFConcat.class);
    registerUDF("substr", UDFSubstr.class);
    registerUDF("str_eq", UDFStrEq.class);
    registerUDF("str_ne", UDFStrNe.class);
    registerUDF("str_gt", UDFStrGt.class);
    registerUDF("str_lt", UDFStrLt.class);
    registerUDF("str_ge", UDFStrGe.class);
    registerUDF("str_le", UDFStrLe.class);

    registerUDF("upper", UDFUpper.class);
    registerUDF("lower", UDFLower.class);
    registerUDF("ucase", UDFUpper.class);
    registerUDF("lcase", UDFLower.class);
    registerUDF("trim", UDFTrim.class);
    registerUDF("ltrim", UDFLTrim.class);
    registerUDF("rtrim", UDFRTrim.class);
    
    registerUDF("like", UDFLike.class);
    registerUDF("rlike", UDFRegExp.class);
    registerUDF("regexp", UDFRegExp.class);
    registerUDF("regexp_replace", UDFRegExpReplace.class);
    
    registerUDF("+", UDFOPPlus.class);
    registerUDF("-", UDFOPMinus.class);
    registerUDF("*", UDFOPMultiply.class);
    registerUDF("/", UDFOPDivide.class);
    registerUDF("%", UDFOPMod.class);
    
    registerUDF("&", UDFOPBitAnd.class);
    registerUDF("|", UDFOPBitOr.class);
    registerUDF("^", UDFOPBitXor.class);
    registerUDF("~", UDFOPBitNot.class);

    registerUDF("=", UDFOPEqual.class);
    registerUDF("==", UDFOPEqual.class);
    registerUDF("<>", UDFOPNotEqual.class);
    registerUDF("<", UDFOPLessThan.class);
    registerUDF("<=", UDFOPEqualOrLessThan.class);
    registerUDF(">", UDFOPGreaterThan.class);
    registerUDF(">=", UDFOPEqualOrGreaterThan.class);

    registerUDF("and", UDFOPAnd.class);
    registerUDF("&&", UDFOPAnd.class);
    registerUDF("or", UDFOPOr.class);
    registerUDF("||", UDFOPOr.class);
    registerUDF("not", UDFOPNot.class);
    registerUDF("!", UDFOPNot.class);

    registerUDF("isnull", UDFOPNull.class);
    registerUDF("isnotnull", UDFOPNotNull.class);
    
    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    registerUDF(Boolean.class.getName(), UDFToBoolean.class);
    registerUDF(Byte.class.getName(), UDFToByte.class);
    registerUDF(Integer.class.getName(), UDFToInteger.class);
    registerUDF(Long.class.getName(), UDFToLong.class);
    registerUDF(Float.class.getName(), UDFToFloat.class);
    registerUDF(Double.class.getName(), UDFToDouble.class);
    registerUDF(String.class.getName(), UDFToString.class);
    registerUDF(java.sql.Date.class.getName(), UDFToDate.class);
  }

  public static void registerUDF(String functionName, Class<? extends UDF> UDFClass) {
    if (UDF.class.isAssignableFrom(UDFClass)) { 
      mUDFs.put(functionName.toLowerCase(), UDFClass);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass + " which does not extends " + UDF.class);
    }
  }
  
  public static Class<? extends UDF> getUDFClass(String functionName) {
    LOG.debug("Looking up: " + functionName);
    Class<? extends UDF> result = mUDFs.get(functionName.toLowerCase());
    return result;
  }

  static Map<Class<?>, Integer> numericTypes;
  static {
    numericTypes = new HashMap<Class<?>, Integer>();
    numericTypes.put(Byte.class, 1);
    numericTypes.put(Integer.class, 2);
    numericTypes.put(Long.class, 3);
    numericTypes.put(Float.class, 4);
    numericTypes.put(Double.class, 5);
    numericTypes.put(String.class, 6);
  } 
  
  /**
   * Find a common class that objects of both Class a and Class b can convert to.
   * @return null if no common class could be found.
   */
  public static Class<?> getCommonClass(Class<?> a, Class<?> b) {
    // Equal
    if (a.equals(b)) return a;
    // Java class inheritance hierarchy
    if (a.isAssignableFrom(b)) return a;
    if (b.isAssignableFrom(a)) return b;
    // Prefer String to Number conversion before implicit conversions
    if (Number.class.isAssignableFrom(a) && b.equals(String.class)) return Double.class;
    if (Number.class.isAssignableFrom(b) && a.equals(String.class)) return Double.class;
    // implicit conversions
    if (UDFRegistry.implicitConvertable(a, b)) return b;
    if (UDFRegistry.implicitConvertable(b, a)) return a;
    return null;
  }
  
  /** Returns whether it is possible to implicitly convert an object of Class from to Class to.
   */
  public static boolean implicitConvertable(Class<?> from, Class<?> to) {
    assert(!from.equals(to));
    // Allow implicit String to Double conversion
    if (from.equals(String.class) && to.equals(Double.class)) {
      return true;
    }
    if (from.equals(String.class) && to.equals(java.sql.Date.class)) {
      return true;
    }
    if (from.equals(java.sql.Date.class) && to.equals(String.class)) {
      return true;
    }
    // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double -> String
    Integer f = numericTypes.get(from);
    Integer t = numericTypes.get(to);
    if (f == null || t == null) return false;
    if (f.intValue() > t.intValue()) return false;
    return true;
  }

  /**
   * Get the UDF method for the name and argumentClasses. 
   * @param name the name of the UDF
   * @param argumentClasses 
   * @param exact  if true, we don't allow implicit type conversions. 
   * @return
   */
  public static Method getUDFMethod(String name, boolean exact, List<Class<?>> argumentClasses) {
    Class<? extends UDF> udf = getUDFClass(name);
    if (udf == null) return null;
    return getMethodInternal(udf, "evaluate", exact, argumentClasses);    
  }
  
  /**
   * This method is shared between UDFRegistry and UDAFRegistry.
   * methodName will be "evaluate" for UDFRegistry, and "aggregate" for UDAFRegistry. 
   */
  public static <T> Method getMethodInternal(Class<? extends T> udfClass, String methodName, boolean exact, List<Class<?>> argumentClasses) {
    int leastImplicitConversions = Integer.MAX_VALUE;
    Method udfMethod = null;
    
    for(Method m: Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals(methodName)) {
        
        Class<?>[] argumentTypeInfos = m.getParameterTypes();
        
        boolean match = (argumentTypeInfos.length == argumentClasses.size());
        int implicitConversions = 0;
        
        for(int i=0; i<argumentClasses.size() && match; i++) {
          if (argumentClasses.get(i) == Void.class) continue;
          Class<?> accepted = TypeInfo.generalizePrimitive(argumentTypeInfos[i]);
          if (accepted.isAssignableFrom(argumentClasses.get(i))) {
            // do nothing if match
          } else if (!exact && implicitConvertable(argumentClasses.get(i), accepted)) {
            implicitConversions ++;
          } else {
            match = false;
          }
        }

        if (match) {
          // Always choose the function with least implicit conversions.
          if (implicitConversions < leastImplicitConversions) {
            udfMethod = m;
            leastImplicitConversions = implicitConversions;
            // Found an exact match
            if (leastImplicitConversions == 0) break;
          } else if (implicitConversions == leastImplicitConversions){
            // Ambiguous call: two methods with the same number of implicit conversions 
            udfMethod = null;
          } else {
            // do nothing if implicitConversions > leastImplicitConversions
          }
        }
      }
    }
    return udfMethod;
  }

  public static Method getUDFMethod(String name, boolean exact, Class<?> ... argumentClasses) {
    return getUDFMethod(name, exact, Arrays.asList(argumentClasses));
  }
  
}
