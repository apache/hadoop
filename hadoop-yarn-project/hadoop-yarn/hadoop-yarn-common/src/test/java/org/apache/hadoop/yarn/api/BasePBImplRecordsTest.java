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
package org.apache.hadoop.yarn.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.junit.Assert;

import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints
    .PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;

/**
 * Generic helper class to validate protocol records.
 */
public class BasePBImplRecordsTest {
  static final Log LOG = LogFactory.getLog(BasePBImplRecordsTest.class);

  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected static HashMap<Type, Object> typeValueCache =
      new HashMap<Type, Object>();
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected static HashMap<Type, List<String>> excludedPropertiesMap =
      new HashMap<>();
  private static Random rand = new Random();
  private static byte [] bytes = new byte[] {'1', '2', '3', '4'};

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object genTypeValue(Type type) {
    Object ret = typeValueCache.get(type);
    if (ret != null) {
      return ret;
    }
    // only use positive primitive values
    if (type.equals(boolean.class)) {
      return rand.nextBoolean();
    } else if (type.equals(byte.class)) {
      return bytes[rand.nextInt(4)];
    } else if (type.equals(int.class) || type.equals(Integer.class)) {
      return rand.nextInt(1000000);
    } else if (type.equals(long.class) || type.equals(Long.class)) {
      return Long.valueOf(rand.nextInt(1000000));
    } else if (type.equals(float.class)) {
      return rand.nextFloat();
    } else if (type.equals(double.class)) {
      return rand.nextDouble();
    } else if (type.equals(String.class)) {
      return String.format("%c%c%c",
          'a' + rand.nextInt(26),
          'a' + rand.nextInt(26),
          'a' + rand.nextInt(26));
    } else if (type instanceof Class) {
      Class clazz = (Class)type;
      if (clazz.isArray()) {
        Class compClass = clazz.getComponentType();
        if (compClass != null) {
          ret = Array.newInstance(compClass, 2);
          Array.set(ret, 0, genTypeValue(compClass));
          Array.set(ret, 1, genTypeValue(compClass));
        }
      } else if (clazz.isEnum()) {
        Object [] values = clazz.getEnumConstants();
        ret = values[rand.nextInt(values.length)];
      } else if (clazz.equals(ByteBuffer.class)) {
        // return new ByteBuffer every time
        // to prevent potential side effects
        ByteBuffer buff = ByteBuffer.allocate(4);
        rand.nextBytes(buff.array());
        return buff;
      } else if (type.equals(PlacementConstraint.class)) {
        PlacementConstraint.AbstractConstraint sConstraintExpr =
            targetIn(NODE, allocationTag("foo"));
        ret = PlacementConstraints.build(sConstraintExpr);
      }
    } else if (type instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType)type;
      Type rawType = pt.getRawType();
      Type [] params = pt.getActualTypeArguments();
      // only support EnumSet<T>, List<T>, Set<T>, Map<K,V>, Range<T>
      if (rawType.equals(EnumSet.class)) {
        if (params[0] instanceof Class) {
          Class c = (Class)(params[0]);
          return EnumSet.allOf(c);
        }
      } if (rawType.equals(List.class)) {
        ret = Lists.newArrayList(genTypeValue(params[0]));
      } else if (rawType.equals(Set.class)) {
        ret = Sets.newHashSet(genTypeValue(params[0]));
      } else if (rawType.equals(Map.class)) {
        Map<Object, Object> map = Maps.newHashMap();
        map.put(genTypeValue(params[0]), genTypeValue(params[1]));
        ret = map;
      } else if (rawType.equals(Range.class)) {
        ret = typeValueCache.get(rawType);
        if (ret != null) {
          return ret;
        }
      }
    }
    if (ret == null) {
      throw new IllegalArgumentException("type " + type + " is not supported");
    }
    typeValueCache.put(type, ret);
    return ret;
  }

  /**
   * this method generate record instance by calling newIntance
   * using reflection, add register the generated value to typeValueCache
   */
  @SuppressWarnings("rawtypes")
  protected static Object generateByNewInstance(Class clazz) throws Exception {
    Object ret = typeValueCache.get(clazz);
    if (ret != null) {
      return ret;
    }
    Method newInstance = null;
    Type [] paramTypes = new Type[0];
    // get newInstance method with most parameters
    for (Method m : clazz.getMethods()) {
      int mod = m.getModifiers();
      if (m.getDeclaringClass().equals(clazz) &&
          Modifier.isPublic(mod) &&
          Modifier.isStatic(mod) &&
          m.getName().equals("newInstance")) {
        Type [] pts = m.getGenericParameterTypes();
        if (newInstance == null
            || (pts.length > paramTypes.length)) {
          newInstance = m;
          paramTypes = pts;
        }
      }
    }
    if (newInstance == null) {
      throw new IllegalArgumentException("type " + clazz.getName() +
          " does not have newInstance method");
    }
    Object [] args = new Object[paramTypes.length];
    for (int i=0;i<args.length;i++) {
      args[i] = genTypeValue(paramTypes[i]);
    }
    ret = newInstance.invoke(null, args);
    typeValueCache.put(clazz, ret);
    return ret;
  }

  private class GetSetPair {
    public String propertyName;
    public Method getMethod;
    public Method setMethod;
    public Type type;
    public Object testValue;

    @Override
    public String toString() {
      return String.format("{ name=%s, class=%s, value=%s }", propertyName,
          type, testValue);
    }
  }

  private <R> Map<String, GetSetPair> getGetSetPairs(Class<R> recordClass)
      throws Exception {
    Map<String, GetSetPair> ret = new HashMap<String, GetSetPair>();
    List<String> excluded = null;
    if (excludedPropertiesMap.containsKey(recordClass.getClass())) {
      excluded = excludedPropertiesMap.get(recordClass.getClass());
    }
    Method [] methods = recordClass.getDeclaredMethods();
    // get all get methods
    for (int i = 0; i < methods.length; i++) {
      Method m = methods[i];
      int mod = m.getModifiers();
      if (m.getDeclaringClass().equals(recordClass) &&
          Modifier.isPublic(mod) &&
          (!Modifier.isStatic(mod))) {
        String name = m.getName();
        if (name.equals("getProto")) {
          continue;
        }
        if ((name.length() > 3) && name.startsWith("get") &&
            (m.getParameterTypes().length == 0)) {
          String propertyName = name.substring(3);
          Type valueType = m.getGenericReturnType();
          GetSetPair p = ret.get(propertyName);
          if (p == null) {
            p = new GetSetPair();
            p.propertyName = propertyName;
            p.type = valueType;
            p.getMethod = m;
            ret.put(propertyName, p);
          } else {
            Assert.fail("Multiple get method with same name: " + recordClass
                + p.propertyName);
          }
        }
      }
    }
    // match get methods with set methods
    for (int i = 0; i < methods.length; i++) {
      Method m = methods[i];
      int mod = m.getModifiers();
      if (m.getDeclaringClass().equals(recordClass) &&
          Modifier.isPublic(mod) &&
          (!Modifier.isStatic(mod))) {
        String name = m.getName();
        if (name.startsWith("set") && (m.getParameterTypes().length == 1)) {
          String propertyName = name.substring(3);
          Type valueType = m.getGenericParameterTypes()[0];
          GetSetPair p = ret.get(propertyName);
          if (p != null && p.type.equals(valueType)) {
            p.setMethod = m;
          }
        }
      }
    }
    // exclude incomplete get/set pair, and generate test value
    Iterator<Map.Entry<String, GetSetPair>> itr = ret.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<String, GetSetPair> cur = itr.next();
      GetSetPair gsp = cur.getValue();
      if ((gsp.getMethod == null) ||
          (gsp.setMethod == null)) {
        LOG.info(String.format("Exclude potential property: %s\n", gsp.propertyName));
        itr.remove();
      } else if ((excluded != null && excluded.contains(gsp.propertyName))) {
        LOG.info(String.format(
            "Excluding potential property(present in exclusion list): %s\n",
            gsp.propertyName));
        itr.remove();
      } else {
        LOG.info(String.format("New property: %s type: %s", gsp.toString(), gsp.type));
        gsp.testValue = genTypeValue(gsp.type);
        LOG.info(String.format(" testValue: %s\n", gsp.testValue));
      }
    }
    return ret;
  }

  protected  <R, P> void validatePBImplRecord(Class<R> recordClass,
      Class<P> protoClass)
      throws Exception {
    LOG.info(String.format("Validate %s %s\n", recordClass.getName(),
        protoClass.getName()));
    Constructor<R> emptyConstructor = recordClass.getConstructor();
    Constructor<R> pbConstructor = recordClass.getConstructor(protoClass);
    Method getProto = recordClass.getDeclaredMethod("getProto");
    Map<String, GetSetPair> getSetPairs = getGetSetPairs(recordClass);
    R origRecord = emptyConstructor.newInstance();
    for (GetSetPair gsp : getSetPairs.values()) {
      gsp.setMethod.invoke(origRecord, gsp.testValue);
    }
    Object ret = getProto.invoke(origRecord);
    Assert.assertNotNull(recordClass.getName() + "#getProto returns null", ret);
    if (!(protoClass.isAssignableFrom(ret.getClass()))) {
      Assert.fail("Illegal getProto method return type: " + ret.getClass());
    }
    R deserRecord = pbConstructor.newInstance(ret);
    Assert.assertEquals("whole " + recordClass + " records should be equal",
        origRecord, deserRecord);
    for (GetSetPair gsp : getSetPairs.values()) {
      Object origValue = gsp.getMethod.invoke(origRecord);
      Object deserValue = gsp.getMethod.invoke(deserRecord);
      Assert.assertEquals("property " + recordClass.getName() + "#"
          + gsp.propertyName + " should be equal", origValue, deserValue);
    }
  }
}
