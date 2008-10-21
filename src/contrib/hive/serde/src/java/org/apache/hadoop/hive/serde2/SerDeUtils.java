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

package org.apache.hadoop.hive.serde2;

import java.util.*;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class SerDeUtils {


  public static final char QUOTE = '"';
  public static final char COLON = ':';
  public static final char COMMA = ',';
  public static final String LBRACKET = "[";
  public static final String RBRACKET = "]";
  public static final String LBRACE = "{";
  public static final String RBRACE = "}";

  private static HashMap<String, Class<?>> serdes = new HashMap<String, Class<?>> ();

  public static void registerSerDe(String name, Class<?> serde) {
    if(serdes.containsKey(name)) {
      throw new RuntimeException("double registering serde " + name);
    }
    serdes.put(name, serde);
  }

  public static Deserializer lookupDeserializer(String name) throws SerDeException {
    Class<?> c;
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
      return (Deserializer)c.newInstance();
    } catch(Exception e) {
      throw new SerDeException(e);
    }
  }


  private static boolean initCoreSerDes = registerCoreSerDes();
  
  protected static boolean registerCoreSerDes() {
    // Eagerly load SerDes so they will register their symbolic names even on Lazy Loading JVMs
    try {
      // loading these classes will automatically register the short names
      Class.forName(org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class.getName());
      Class.forName(org.apache.hadoop.hive.serde2.ThriftDeserializer.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("IMPOSSIBLE Exception: Unable to initialize core serdes", e);
    }
    return true;
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

  public static String getJSONString(Object o, ObjectInspector oi) {
    StringBuilder sb = new StringBuilder();
    buildJSONString(sb, o, oi);
    return sb.toString();
  }

  
  static void buildJSONString(StringBuilder sb, Object o, ObjectInspector oi) {

    switch(oi.getCategory()) {
      case PRIMITIVE: {
        if (o == null) {
          sb.append("null");
        } else if (o instanceof String) {
          sb.append(QUOTE);
          sb.append(escapeString((String)o));
          sb.append(QUOTE);
        } else if (o instanceof Boolean) {
          sb.append(((Boolean)o).booleanValue() ? "True" : "False");
        } else {
          // it's a number - so doesn't need to be escaped.
          sb.append(o.toString());
        }
        break;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        ObjectInspector listElementObjectInspector = loi.getListElementObjectInspector();
        List<?> olist = loi.getList(o);
        if (olist == null) {
          sb.append("null");
        } else {
          sb.append(LBRACKET);
          for (int i=0; i<olist.size(); i++) {
            if (i>0) sb.append(COMMA);
            buildJSONString(sb, olist.get(i), listElementObjectInspector);
          }
          sb.append(RBRACKET);
        }
        break;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        ObjectInspector mapKeyObjectInspector = moi.getMapKeyObjectInspector();
        ObjectInspector mapValueObjectInspector = moi.getMapValueObjectInspector();
        Map<?,?> omap = moi.getMap(o);
        if (omap == null) {
          sb.append("null");
        } else {
          sb.append(LBRACE);
          boolean first = true;
          for(Object entry : omap.entrySet()) {
            if (first) {
              first = false;
            } else {
              sb.append(COMMA);
            }
            Map.Entry<?,?> e = (Map.Entry<?,?>)entry;
            buildJSONString(sb, e.getKey(), mapKeyObjectInspector);
            sb.append(COLON);
            buildJSONString(sb, e.getValue(), mapValueObjectInspector);
          }
          sb.append(RBRACE);
        }
        break;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> structFields = soi.getAllStructFieldRefs();
        if (structFields == null) {
          sb.append("null");
        } else {
          sb.append(LBRACE);
          for(int i=0; i<structFields.size(); i++) {
            if (i>0) {
              sb.append(COMMA);
            }
            sb.append(QUOTE);
            sb.append(structFields.get(i).getFieldName());
            sb.append(QUOTE);
            sb.append(COLON);
            buildJSONString(sb, soi.getStructFieldData(o, structFields.get(i)), 
                structFields.get(i).getFieldObjectInspector());          
          }
          sb.append(RBRACE);
        }
        break;
      }
      default:
        throw new RuntimeException("Unknown type in ObjectInspector!");
    };
    
  }  
}
