/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * A container for Result objects, grouped by regionName.
 */
public class MultiResponse implements Writable {

  // map of regionName to list of (Results paired to the original index for that
  // Result)
  private Map<byte[], List<Pair<Integer, Object>>> results =
      new TreeMap<byte[], List<Pair<Integer, Object>>>(Bytes.BYTES_COMPARATOR);

  public MultiResponse() {
  }

  /**
   * @return Number of pairs in this container
   */
  public int size() {
    int size = 0;
    for (Collection<?> c : results.values()) {
      size += c.size();
    }
    return size;
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName
   * @param r
   *          First item in the pair is the original index of the Action
   *          (request). Second item is the Result. Result will be empty for
   *          successful Put and Delete actions.
   */
  public void add(byte[] regionName, Pair<Integer, Object> r) {
    List<Pair<Integer, Object>> rs = results.get(regionName);
    if (rs == null) {
      rs = new ArrayList<Pair<Integer, Object>>();
      results.put(regionName, rs);
    }
    rs.add(r);
  }

  public void add(byte []regionName, int originalIndex, Object resOrEx) {
    add(regionName, new Pair<Integer,Object>(originalIndex, resOrEx));
  }

  public Map<byte[], List<Pair<Integer, Object>>> getResults() {
    return results;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(results.size());
    for (Map.Entry<byte[], List<Pair<Integer, Object>>> e : results.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      List<Pair<Integer, Object>> lst = e.getValue();
      out.writeInt(lst.size());
      for (Pair<Integer, Object> r : lst) {
        if (r == null) {
          out.writeInt(-1); // Cant have index -1; on other side we recognize -1 as 'null'
        } else {
          out.writeInt(r.getFirst()); // Can this can npe!?!
          Object obj = r.getSecond();
          if (obj instanceof Throwable) {
            out.writeBoolean(true); // true, Throwable/exception.

            Throwable t = (Throwable) obj;
            // serialize exception
            WritableUtils.writeString(out, t.getClass().getName());
            WritableUtils.writeString(out,
                StringUtils.stringifyException(t));

          } else {
            out.writeBoolean(false); // no exception

            if (! (obj instanceof Writable))
              obj = null; // squash all non-writables to null.
            HbaseObjectWritable.writeObject(out, obj, Result.class, null);
          }
        }
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    results.clear();
    int mapSize = in.readInt();
    for (int i = 0; i < mapSize; i++) {
      byte[] key = Bytes.readByteArray(in);
      int listSize = in.readInt();
      List<Pair<Integer, Object>> lst = new ArrayList<Pair<Integer, Object>>(
          listSize);
      for (int j = 0; j < listSize; j++) {
        Integer idx = in.readInt();
        if (idx == -1) {
          lst.add(null);
        } else {
          boolean isException = in.readBoolean();
          Object o = null;
          if (isException) {
            String klass = WritableUtils.readString(in);
            String desc = WritableUtils.readString(in);
            try {
              // the type-unsafe insertion, but since we control what klass is..
              Class<? extends Throwable> c = (Class<? extends Throwable>) Class.forName(klass);
              Constructor<? extends Throwable> cn = c.getDeclaredConstructor(String.class);
              o = cn.newInstance(desc);
            } catch (ClassNotFoundException ignored) {
            } catch (NoSuchMethodException ignored) {
            } catch (InvocationTargetException ignored) {
            } catch (InstantiationException ignored) {
            } catch (IllegalAccessException ignored) {
            }
          } else {
            o = HbaseObjectWritable.readObject(in, null);
          }
          lst.add(new Pair<Integer, Object>(idx, o));
        }
      }
      results.put(key, lst);
    }
  }

}
