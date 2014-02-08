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
package org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A utility class providing methods for serializing and deserializing
 * objects. The {@link #write(Object)}, {@link #read(byte[])} and {@link
 * #write(java.io.DataOutputStream, Object)}, {@link
 * #read(java.io.DataInputStream)} methods are used by the
 * {@link LeveldbApplicationTimelineStore} to store and retrieve arbitrary
 * JSON, while the {@link #writeReverseOrderedLong} and {@link
 * #readReverseOrderedLong} methods are used to sort entities in descending
 * start time order.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GenericObjectMapper {
  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final byte LONG = 0x1;
  private static final byte INTEGER = 0x2;
  private static final byte DOUBLE = 0x3;
  private static final byte STRING = 0x4;
  private static final byte BOOLEAN = 0x5;
  private static final byte LIST = 0x6;
  private static final byte MAP = 0x7;

  /**
   * Serializes an Object into a byte array. Along with {@link #read(byte[]) },
   * can be used to serialize an Object and deserialize it into an Object of
   * the same type without needing to specify the Object's type,
   * as long as it is one of the JSON-compatible objects Long, Integer,
   * Double, String, Boolean, List, or Map.  The current implementation uses
   * ObjectMapper to serialize complex objects (List and Map) while using
   * Writable to serialize simpler objects, to produce fewer bytes.
   *
   * @param o An Object
   * @return A byte array representation of the Object
   * @throws IOException
   */
  public static byte[] write(Object o) throws IOException {
    if (o == null)
      return EMPTY_BYTES;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    write(new DataOutputStream(baos), o);
    return baos.toByteArray();
  }

  /**
   * Serializes an Object and writes it to a DataOutputStream. Along with
   * {@link #read(java.io.DataInputStream)}, can be used to serialize an Object
   * and deserialize it into an Object of the same type without needing to
   * specify the Object's type, as long as it is one of the JSON-compatible
   * objects Long, Integer, Double, String, Boolean, List, or Map. The current
   * implementation uses ObjectMapper to serialize complex objects (List and
   * Map) while using Writable to serialize simpler objects, to produce fewer
   * bytes.
   *
   * @param dos A DataOutputStream
   * @param o An Object
   * @throws IOException
   */
  public static void write(DataOutputStream dos, Object o)
      throws IOException {
    if (o == null)
      return;
    if (o instanceof Long) {
      dos.write(LONG);
      WritableUtils.writeVLong(dos, (Long) o);
    } else if(o instanceof Integer) {
      dos.write(INTEGER);
      WritableUtils.writeVInt(dos, (Integer) o);
    } else if(o instanceof Double) {
      dos.write(DOUBLE);
      dos.writeDouble((Double) o);
    } else if (o instanceof String) {
      dos.write(STRING);
      WritableUtils.writeString(dos, (String) o);
    } else if (o instanceof Boolean) {
      dos.write(BOOLEAN);
      dos.writeBoolean((Boolean) o);
    } else if (o instanceof List) {
      dos.write(LIST);
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(dos, o);
    } else if (o instanceof Map) {
      dos.write(MAP);
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(dos, o);
    } else {
      throw new IOException("Couldn't serialize object");
    }
  }

  /**
   * Deserializes an Object from a byte array created with
   * {@link #write(Object)}.
   *
   * @param b A byte array
   * @return An Object
   * @throws IOException
   */
  public static Object read(byte[] b) throws IOException {
    if (b == null || b.length == 0)
      return null;
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    return read(new DataInputStream(bais));
  }

  /**
   * Reads an Object from a DataInputStream whose data has been written with
   * {@link #write(java.io.DataOutputStream, Object)}.
   *
   * @param dis A DataInputStream
   * @return An Object, null if an unrecognized type
   * @throws IOException
   */
  public static Object read(DataInputStream dis) throws IOException {
    byte code = (byte)dis.read();
    ObjectMapper mapper;
    switch (code) {
      case LONG:
        return WritableUtils.readVLong(dis);
      case INTEGER:
        return WritableUtils.readVInt(dis);
      case DOUBLE:
        return dis.readDouble();
      case STRING:
        return WritableUtils.readString(dis);
      case BOOLEAN:
        return dis.readBoolean();
      case LIST:
        mapper = new ObjectMapper();
        return mapper.readValue(dis, ArrayList.class);
      case MAP:
        mapper = new ObjectMapper();
        return mapper.readValue(dis, HashMap.class);
      default:
        return null;
    }
  }

  /**
   * Converts a long to a 8-byte array so that lexicographic ordering of the
   * produced byte arrays sort the longs in descending order.
   *
   * @param l A long
   * @return A byte array
   */
  public static byte[] writeReverseOrderedLong(long l) {
    byte[] b = new byte[8];
    b[0] = (byte)(0x7f ^ ((l >> 56) & 0xff));
    for (int i = 1; i < 7; i++)
      b[i] = (byte)(0xff ^ ((l >> 8*(7-i)) & 0xff));
    b[7] = (byte)(0xff ^ (l & 0xff));
    return b;
  }

  /**
   * Reads 8 bytes from an array starting at the specified offset and
   * converts them to a long.  The bytes are assumed to have been created
   * with {@link #writeReverseOrderedLong}.
   *
   * @param b A byte array
   * @param offset An offset into the byte array
   * @return A long
   */
  public static long readReverseOrderedLong(byte[] b, int offset) {
    long l = b[offset] & 0xff;
    for (int i = 1; i < 8; i++) {
      l = l << 8;
      l = l | (b[offset+i]&0xff);
    }
    return l ^ 0x7fffffffffffffffl;
  }

}
