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

import java.util.*;

public class SerDeUtils {


  public static final char QUOTE = '"';
  public static final char COLON = ':';
  public static final String LBRACKET = "[";
  public static final String RBRACKET = "]";
  public static final String LBRACE = "{";
  public static final String RBRACE = "}";

  private static HashMap<String, Class> serdes = new HashMap<String, Class> ();

  public static void registerSerDe(String name, Class serde) {
    if(serdes.containsKey(name)) {
      throw new RuntimeException("double registering serde " + name);
    }
    serdes.put(name, serde);
  }

  public static SerDe lookupSerDe(String name) throws SerDeException {
    Class c;
    if(serdes.containsKey(name)) {
        c = serdes.get(name);
    } else {
      try {
        c = Class.forName(name);
      } catch(ClassNotFoundException e) {
        throw new SerDeException("SerDe " + name + " does not exist");
      }
    }
    try {
      return (SerDe)c.newInstance();
    } catch(Exception e) {
      throw new SerDeException(e);
    }
  }


  private static boolean initCoreSerDes = registerCoreSerDes();
  
  protected static boolean registerCoreSerDes() {
    // Eagerly load SerDes so they will register their symbolic names even on Lazy Loading JVMs
    try {
      // loading these classes will automatically register the short names
      Class.forName(org.apache.hadoop.hive.serde.dynamic_type.DynamicSerDe.class.getName());
      Class.forName(org.apache.hadoop.hive.serde.jute.JuteSerDe.class.getName());
      Class.forName(org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe.class.getName());
      Class.forName(org.apache.hadoop.hive.serde.thrift.columnsetSerDe.class.getName());
      Class.forName(org.apache.hadoop.hive.serde.thrift.ThriftSerDe.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("IMPOSSIBLE Exception: Unable to initialize core serdes", e);
    }
    return true;
  }

  /**
   * Common functionality for all SerDe libraries to handle list and primitive
   * string serialization to json strings
   */
  public static String toJSONString(Object obj, SerDeField hf, SerDe sd) throws SerDeException {

    if(hf.isList()) {
      Class type = hf.getListElementType();
      boolean is_string = (type == String.class);
      boolean is_boolean = (type == Boolean.class);
      boolean is_primitive = ReflectionSerDeField.isClassPrimitive(type);

      Iterator iter = ((List)obj).iterator();
      StringBuilder sb = new StringBuilder(LBRACKET);
      String toPrefix = "";
      boolean first = true;

      while(iter.hasNext()) {
        Object lobj = iter.next();

        if(is_primitive) {
          if(is_string) {
            sb.append(toPrefix);
            sb.append(QUOTE);
            sb.append(escapeString((String)lobj));
            sb.append(QUOTE);
          } else if(is_boolean) {
            sb.append(toPrefix+(((Boolean)lobj).booleanValue() ? "True" : "False"));
          } else {
            // it's a number - so doesn't need to be escaped.
            sb.append(toPrefix+lobj.toString());
          }
        } else {
          sb.append(toPrefix+sd.toJSONString(lobj, null));
        }

        if(first) {
          toPrefix = ",";
          first = false;
        }
      }
      sb.append(RBRACKET);
      return (sb.toString());
    } else if (hf.isMap()) {
      Class keyType = hf.getMapKeyType();
      Class valType = hf.getMapValueType();

      boolean key_is_string = (keyType == String.class);
      boolean key_is_boolean = (keyType == Boolean.class);
      boolean key_is_primitive = ReflectionSerDeField.isClassPrimitive(keyType);

      boolean val_is_string = (valType == String.class);
      boolean val_is_boolean = (valType == Boolean.class);
      boolean val_is_primitive = ReflectionSerDeField.isClassPrimitive(valType);



      Iterator iter = ((Map)obj).keySet().iterator();
      StringBuilder sb = new StringBuilder(LBRACE);
      String toPrefix = "";
      boolean first = true;

      while(iter.hasNext()) {
        Object lobj = iter.next();
        Object robj = ((Map)obj).get(lobj);

        // Emit key
        if(key_is_primitive) {
          if(key_is_string) {
            sb.append(toPrefix);
            sb.append(QUOTE);
            sb.append(escapeString((String)lobj));
            sb.append(QUOTE);
          } else if(key_is_boolean) {
            sb.append(toPrefix+(((Boolean)lobj).booleanValue() ? "True" : "False"));
          } else {
            // it's a number - so doesn't need to be escaped.
            sb.append(toPrefix+lobj.toString());
          }
        } else {
          sb.append(toPrefix+sd.toJSONString(lobj, null));
        }

        sb.append(COLON);

        // Emit val
        if(val_is_primitive) {
          if(val_is_string) {
            sb.append(toPrefix);
            sb.append(QUOTE);
            sb.append(escapeString((String)robj));
            sb.append(QUOTE);
          } else if(val_is_boolean) {
            sb.append(toPrefix+(((Boolean)robj).booleanValue() ? "True" : "False"));
          } else {
            // it's a number - so doesn't need to be escaped.
            sb.append(toPrefix+robj.toString());
          }
        } else {
          sb.append(toPrefix+sd.toJSONString(robj, null));
        }

        if(first) {
          toPrefix = ",";
          first = false;
        }
      }
      sb.append(RBRACE);
      return (sb.toString());
    } else {
      throw new SerDeException("SerDeUtils.toJSONString only does lists");
    }
  }

  public static String escapeString(String str) {
    int length = str.length();
    StringBuilder escape = new StringBuilder(length + 16);

    for (int i = 0; i < length; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '"':
      case '\\':
        escape.append('\\');
        escape.append(c);
        break;
      case '\b':
        escape.append('\\');
        escape.append('b');
        break;
      case '\f':
        escape.append('\\');
        escape.append('f');
        break;
      case '\n':
        escape.append('\\');
        escape.append('n');
        break;
      case '\r':
        escape.append('\\');
        escape.append('r');
        break;
      case '\t':
        escape.append('\\');
        escape.append('t');
        break;
      default:
        // Control characeters! According to JSON RFC u0020
        if (c < ' ') {
          String hex = Integer.toHexString(c);
          escape.append('\\');
          escape.append('u');
          for (int j = 4; j > hex.length(); --j) {
            escape.append('0');
          }
          escape.append(hex);
        } else {
          escape.append(c);
        }
        break;
      }
    }
    return (escape.toString());
  }


  public static String lightEscapeString(String str) {
    int length = str.length();
    StringBuilder escape = new StringBuilder(length + 16);

    for (int i = 0; i < length; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '\n':
        escape.append('\\');
        escape.append('n');
        break;
      case '\r':
        escape.append('\\');
        escape.append('r');
        break;
      case '\t':
        escape.append('\\');
        escape.append('t');
        break;
      default:
        escape.append(c);
        break;
      }
    }
    return (escape.toString());
  }

}
