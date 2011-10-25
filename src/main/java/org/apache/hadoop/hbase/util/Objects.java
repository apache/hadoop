/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Put;

/**
 * Utility methods for interacting with object instances.
 */
public class Objects {
  private static class QuantityMap extends HashMap<String,Quantity> {
    public void increment(String type, int count) {
      Quantity q = get(type);
      if (q == null) {
        q = new Quantity();
        q.what = type;
        put(type, q);
      }
      q.increment(count);
    }

    public void stat(String type, int value) {
      Quantity q = get(type);
      if (q == null) {
        q = new Stat();
        q.what = type;
        put(type, q);
      }
      q.increment(value);
    }
  }

  private static class Quantity {
    int count;
    String what;

    public void increment(int amount) {
      count += amount;
    }

    public void appendToString(StringBuilder out) {
      if (out.length() > 0) out.append(", ");

      out.append(count).append(" ").append(what);
      if (count != 1 && !what.endsWith("s")) {
        out.append("s");
      }
    }
  }

  private static class Stat extends Quantity {
    int min;
    int max;
    long total;

    public void increment(int amount) {
      if (count == 0) {
        min = max = amount;
      } else {
        min = Math.min(min, amount);
        max = Math.max(max, amount);
      }
      total += amount;
      count++;
    }

    public void appendToString(StringBuilder out) {
      super.appendToString(out);

      out.append(" [ ");
      if (count > 0) {
        out.append("min=").append(min)
            .append(" max=").append(max)
            .append(" avg=").append((int)(total/count));
      } else {
        out.append("none");
      }
      out.append(" ]");
    }
  }

  private static class QuantityComparator implements Comparator<Quantity> {
    @Override
    public int compare(Quantity q1, Quantity q2) {
      if (q1.count < q2.count) {
        return -1;
      } else if (q1.count > q2.count) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Attempts to construct a text description of the given object, by
   * introspecting known classes and building a description of size.
   * @param obj
   * @return Description
   */
  public static String describeQuantity(Object obj) {
    StringBuilder str = new StringBuilder();
    QuantityMap quantities = new QuantityMap();
    quantify(obj, quantities);
    List<Quantity> totals = new ArrayList<Quantity>(quantities.values());
    Collections.sort(totals, new QuantityComparator());
    for (Quantity q : totals) {
      q.appendToString(str);
    }
    return str.toString();
  }

  public static void quantify(Object obj, QuantityMap quantities) {
    if (obj == null) {
      return;
    }

    if (obj.getClass().isArray()) {
      Class type = obj.getClass().getComponentType();
      int length = Array.getLength(obj);
      if (type.isPrimitive()) {
        quantities.increment(type.getSimpleName(), length);
      } else {
        for (int i=0; i<length; i++) {
          quantify(Array.get(obj, i), quantities);
        }
      }
    } else if (obj instanceof Iterable) {
      for (Object child : ((Iterable)obj)) {
        quantify(child, quantities);
      }
    } else if (obj instanceof MultiAction) {
      MultiAction multi = (MultiAction)obj;
      quantify(multi.allActions(), quantities);
    } else if (obj instanceof Action) {
      quantify(((Action)obj).getAction(), quantities);
    } else if (obj instanceof Put) {
      quantities.increment("Put", 1);
      quantities.increment("KeyValue", ((Put)obj).size());
      for (List<KeyValue> keyValues : ((Put)obj).getFamilyMap().values()) {
        for (KeyValue kv : keyValues) {
          quantities.stat("values", kv.getValueLength());
        }
      }
    } else if (obj instanceof Delete) {
      quantities.increment("Delete", 1);
      for (List<KeyValue> kvs : ((Delete)obj).getFamilyMap().values()) {
        quantities.increment("KeyValue", kvs.size());
      }
    } else if (obj instanceof Increment) {
      quantities.increment("Increment", 1);
      quantities.increment("KeyValue", ((Increment)obj).numColumns());
    } else if (obj instanceof Get) {
      quantities.increment("Get", 1);
    } else {
      String type = obj.getClass().getSimpleName();
      quantities.increment(type, 1);
    }
  }
}
