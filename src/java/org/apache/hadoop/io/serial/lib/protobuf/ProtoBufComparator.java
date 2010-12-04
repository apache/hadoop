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
package org.apache.hadoop.io.serial.lib.protobuf;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.serial.RawComparator;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

public class ProtoBufComparator implements RawComparator {
  static final int WIRETYPE_VARINT           = 0;
  static final int WIRETYPE_FIXED64          = 1;
  static final int WIRETYPE_LENGTH_DELIMITED = 2;
  static final int WIRETYPE_START_GROUP      = 3;
  static final int WIRETYPE_END_GROUP        = 4;
  static final int WIRETYPE_FIXED32          = 5;

  static final int TAG_TYPE_BITS = 3;
  static final int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;

  private final Map<Descriptor, List<FieldDescriptor>> fieldCache = 
    new HashMap<Descriptor, List<FieldDescriptor>>();
  private final List<FieldDescriptor> topFields;

  /**
   * Create a comparator that will compare serialized messages of a particular
   * class.
   * @param cls the specific class to compare
   */
  public ProtoBufComparator(Class<? extends Message> cls) {
    if (!Message.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Type " + cls + 
      		                               "must be a generated protobuf class");
    }
    try {
      Method getDescriptor = cls.getDeclaredMethod("getDescriptor");
      topFields = addToCache((Descriptor) getDescriptor.invoke(null));
    } catch (Exception e) {
      throw new IllegalArgumentException("Can't get descriptors for " + cls, e);
    }
  }

  /**
   * Define a comparator so that we can sort the fields by their field ids.
   */
  private static class FieldIdComparator implements Comparator<FieldDescriptor>{

    @Override
    public int compare(FieldDescriptor left, FieldDescriptor right) {
      int leftId = left.getNumber();
      int rightId = right.getNumber();
      if (leftId == rightId) {
        return 0;
      } else {
        return leftId < rightId ? -1 : 1;
      }
    }
  }

  private static final FieldIdComparator FIELD_COMPARE= new FieldIdComparator();

  /**
   * Add all of the types that are recursively nested under the given one
   * to the cache. The fields are sorted by field id.
   * @param type the type to add
   * @return the list of sorted fields for the given type
   */
  private List<FieldDescriptor> addToCache(Descriptor type) {
    List<FieldDescriptor> fields = 
      new ArrayList<FieldDescriptor>(type.getFields());
    Collections.sort(fields, FIELD_COMPARE);
    fieldCache.put(type, fields);
    for(Descriptor subMessage: type.getNestedTypes()) {
      if (!fieldCache.containsKey(subMessage)) {
        addToCache(subMessage);
      }
    }
    return fields;
  }

  /**
   * Compare two serialized protocol buffers using the natural sort order.
   * @param b1 the left serialized value
   * @param s1 the first byte index in b1 to compare
   * @param l1 the number of bytes in b1 to compare
   * @param b2 the right serialized value
   * @param s2 the first byte index in b2 to compare
   * @param l2 the number of bytes in b2 to compare
   */
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    CodedInputStream left = CodedInputStream.newInstance(b1,s1,l1);
    CodedInputStream right = CodedInputStream.newInstance(b2, s2, l2);
    try {
      return compareMessage(left, right, topFields);
    } catch (IOException ie) {
      throw new IllegalArgumentException("problem running compare", ie);
    }
  }

  /**
   * Advance the stream to the given fieldId or one that is larger.
   * @param stream the stream to read
   * @param currentTag the last tag that was read from this stream
   * @param fieldId the id of the field we are looking for
   * @return the last tag that was read or 0 for end of stream
   * @throws IOException
   */
  private int advanceTo(CodedInputStream stream, 
                        int currentTag,
                        int fieldId) throws IOException {
    int goal = fieldId << TAG_TYPE_BITS;
    // if we've already seen the right tag, return it
    if (currentTag > goal) {
      return currentTag;
    }
    while (!stream.isAtEnd()) {
      currentTag = stream.readTag();
      if (currentTag > goal) {
        return currentTag;
      } else {
        stream.skipField(currentTag);
      }
    }
    return 0;
  }

  /**
   * Check compatibility between the logical type in the schema and the
   * wiretype. Incompatible fields are ignored.
   * @param tag the tag (id and wiretype) of the field
   * @param type the logical type of the field
   * @return true if we should use this field for comparing
   */
  private boolean isCompatible(int tag, Type type) {
    int wiretype = tag & TAG_TYPE_MASK;
    switch (type) {
    case BOOL:
    case ENUM:
    case INT32:
    case INT64:
    case SINT32:
    case SINT64:
    case UINT32:
    case UINT64:
      return wiretype == WIRETYPE_VARINT || 
             wiretype == WIRETYPE_LENGTH_DELIMITED;

    case BYTES:
    case MESSAGE:
    case STRING:
      return wiretype == WIRETYPE_LENGTH_DELIMITED;

    case FLOAT:
    case FIXED32:
    case SFIXED32:
      return wiretype == WIRETYPE_LENGTH_DELIMITED ||
             wiretype == WIRETYPE_FIXED32;

    case DOUBLE:
    case SFIXED64:
    case FIXED64:
      return wiretype == WIRETYPE_LENGTH_DELIMITED || 
             wiretype == WIRETYPE_FIXED64;

    case GROUP:
      // don't bother dealing with groups, since they aren't used outside of
      // the protobuf mothership.
      return false;

    default:
      throw new IllegalArgumentException("Unknown field type " + type);
    }
  }

  /**
   * Compare two serialized messages of the same type.
   * @param left the left message
   * @param right the right message
   * @param fields the fields of the message
   * @return -1, 0, or 1 if left is less, equal or greater than right
   * @throws IOException
   */
  private int compareMessage(CodedInputStream left, CodedInputStream right,
                             List<FieldDescriptor> fields
                            ) throws IOException {
    int leftTag = 0;
    int rightTag = 0;
    for(FieldDescriptor field: fields) {
      int fieldId = field.getNumber();
      Type fieldType = field.getType();
      int wireFormat = 0;
      leftTag = advanceTo(left, leftTag, fieldId);
      rightTag = advanceTo(right, rightTag, fieldId);
      boolean leftDefault = (leftTag >>> TAG_TYPE_BITS) != fieldId;
      boolean rightDefault = (rightTag >>> TAG_TYPE_BITS) != fieldId;
      // if the fieldType and wiretypes aren't compatible, skip field
      if (!leftDefault && !isCompatible(leftTag, fieldType)) {
        leftDefault = true;
        left.skipField(leftTag);
      }
      if (!rightDefault && !isCompatible(rightTag, fieldType)) {
        rightDefault = true;
        right.skipField(leftTag);
      }
      if (!leftDefault) {
        wireFormat = leftTag & TAG_TYPE_MASK;
        // ensure both sides use the same representation
        if (!rightDefault && leftTag != rightTag) {
          return leftTag < rightTag ? -1 : 1;
        }
      } else if (rightDefault) {
        continue;
      }
      int result;
      switch (wireFormat) {
      case WIRETYPE_LENGTH_DELIMITED:
        switch (fieldType) {
        case STRING:
          String leftStr = 
            leftDefault ? (String) field.getDefaultValue() : left.readString();
          String rightStr = 
            rightDefault ? (String) field.getDefaultValue() :right.readString();
          result = leftStr.compareTo(rightStr);
          if (result != 0) {
            return result;
          }
          break;
        case BYTES:
          result = compareBytes(leftDefault ? 
                                  (ByteString) field.getDefaultValue() : 
                                  left.readBytes(), 
                                rightDefault ?
                                  (ByteString) field.getDefaultValue() :
                                  right.readBytes());
          if (result != 0) {
            return result;
          }
          break;
        default:
          // handle nested messages and packed fields
          if (leftDefault) {
            return -1;
          } else if (rightDefault) {
            return 1;
          }
          int leftLimit = left.readRawVarint32();
          int rightLimit = right.readRawVarint32();
          int oldLeft = left.pushLimit(leftLimit);
          int oldRight = right.pushLimit(rightLimit);
          while (left.getBytesUntilLimit() > 0 &&
              right.getBytesUntilLimit() > 0) {
            result = compareField(field, left, right, false, false);
            if (result != 0) {
              return result;
            }
          }
          if (right.getBytesUntilLimit() > 0) {
            return -1;
          } else if (left.getBytesUntilLimit() > 0) {
            return 1;
          }
          left.popLimit(oldLeft);
          right.popLimit(oldRight); 
        }
        break;
      case WIRETYPE_VARINT:
      case WIRETYPE_FIXED32:
      case WIRETYPE_FIXED64:
        result = compareField(field, left, right, leftDefault, rightDefault);
        if (result != 0) {
          return result;
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown field encoding " + 
                                           wireFormat);
      }
    }
    return 0;
  }

  /**
   * Compare a single field inside of a message. This is used for both packed
   * and unpacked fields. It assumes the wire type has already been checked.
   * @param field the type of the field that we are comparing
   * @param left the left value
   * @param right the right value
   * @param leftDefault use the default value instead of the left value
   * @param rightDefault use the default value instead of the right value
   * @return -1, 0, 1 depending on whether left is less, equal or greater than
   *         right
   * @throws IOException
   */
  private int compareField(FieldDescriptor field,
                           CodedInputStream left,
                           CodedInputStream right,
                           boolean leftDefault,
                           boolean rightDefault) throws IOException {
    switch (field.getType()) {
    case BOOL:
      boolean leftBool = leftDefault ?
                           (Boolean) field.getDefaultValue() : left.readBool();
      boolean rightBool = rightDefault ?
                           (Boolean) field.getDefaultValue() : right.readBool();
      if (leftBool == rightBool) {
        return 0;
      } else {
        return rightBool ? -1 : 1;
      }
    case DOUBLE:
      return 
         Double.compare(leftDefault ? 
                          (Double) field.getDefaultValue(): left.readDouble(), 
                        rightDefault ? 
                          (Double) field.getDefaultValue() :right.readDouble());
    case ENUM:
      return compareInt32(leftDefault ? intDefault(field) : left.readEnum(), 
                          rightDefault ? intDefault(field) : right.readEnum());
    case FIXED32:
      return compareUInt32(leftDefault ? intDefault(field) : left.readFixed32(), 
                           rightDefault?intDefault(field):right.readFixed32());
    case FIXED64:
      return compareUInt64(leftDefault? longDefault(field) : left.readFixed64(), 
                           rightDefault?longDefault(field):right.readFixed64());
    case FLOAT:
      return Float.compare(leftDefault ? 
                             (Float) field.getDefaultValue() : left.readFloat(), 
                           rightDefault ?
                             (Float) field.getDefaultValue():right.readFloat());
    case INT32:
      return compareInt32(leftDefault?intDefault(field):left.readInt32(), 
                          rightDefault?intDefault(field):right.readInt32());
    case INT64:
      return compareInt64(leftDefault?longDefault(field):left.readInt64(), 
                          rightDefault?longDefault(field):right.readInt64());
    case MESSAGE:
      return compareMessage(left, right, 
                            fieldCache.get(field.getMessageType()));
    case SFIXED32:
      return compareInt32(leftDefault ?intDefault(field):left.readSFixed32(),
                          rightDefault ?intDefault(field):right.readSFixed32());
    case SFIXED64:
      return compareInt64(leftDefault ? longDefault(field) :left.readSFixed64(),
                          rightDefault?longDefault(field):right.readSFixed64());
    case SINT32:
      return compareInt32(leftDefault?intDefault(field):left.readSInt32(),
                          rightDefault?intDefault(field):right.readSInt32());
    case SINT64:
      return compareInt64(leftDefault ? longDefault(field) : left.readSInt64(), 
                          rightDefault?longDefault(field):right.readSInt64());
    case UINT32:
      return compareUInt32(leftDefault ? intDefault(field) :left.readUInt32(), 
                           rightDefault ? intDefault(field):right.readUInt32());
    case UINT64:
      return compareUInt64(leftDefault ? longDefault(field) :left.readUInt64(),
                           rightDefault?longDefault(field) :right.readUInt64());
    default:
      throw new IllegalArgumentException("unknown field type " + field);  
    }
  }
  
  /**
   * Compare 32 bit signed integers.
   * @param left
   * @param right
   * @return -1, 0 ,1 if left is less, equal, or greater to right
   */
  private static int compareInt32(int left, int right) {
    if (left == right) {
      return 0;
    } else {
      return left < right ? -1 : 1;
    }
  }
  
  /**
   * Compare 64 bit signed integers.
   * @param left
   * @param right
   * @return -1, 0 ,1 if left is less, equal, or greater to right
   */
  private static int compareInt64(long left, long right) {
    if (left == right) {
      return 0;
    } else {
      return left < right ? -1 : 1;
    }
  }
  
  /**
   * Compare 32 bit logically unsigned integers.
   * @param left
   * @param right
   * @return -1, 0 ,1 if left is less, equal, or greater to right
   */
  private static int compareUInt32(int left, int right) {
    if (left == right) {
      return 0;
    } else {
      return left + Integer.MIN_VALUE < right + Integer.MIN_VALUE ? -1 : 1;
    }
  }

  /**
   * Compare two byte strings using memcmp semantics
   * @param left
   * @param right
   * @return -1, 0, 1 if left is less, equal, or greater than right
   */
  private static int compareBytes(ByteString left, ByteString right) {
    int size = Math.min(left.size(), right.size());
    for(int i = 0; i < size; ++i) {
      int leftByte = left.byteAt(i) & 0xff;
      int rightByte = right.byteAt(i) & 0xff;
      if (leftByte != rightByte) {
        return leftByte < rightByte ? -1 : 1;
      }
    }
    if (left.size() != right.size()) {
      return left.size() < right.size() ? -1 : 1;
    }
    return 0;
  }

  /**
   * Compare 32 bit logically unsigned integers.
   * @param left
   * @param right
   * @return -1, 0 ,1 if left is less, equal, or greater to right
   */
  private static int compareUInt64(long left, long right) {
    if (left == right) {
      return 0;
    } else {
      return left + Long.MIN_VALUE < right + Long.MIN_VALUE ? -1 : 1;
    }
  }

  /**
   * Get the integer default, including dereferencing enum values.
   * @param field the field
   * @return the integer default value
   */
  private static int intDefault(FieldDescriptor field) {
    if (field.getType() == Type.ENUM) {
      return ((EnumValueDescriptor) field.getDefaultValue()).getNumber();
    } else {
      return (Integer) field.getDefaultValue();
    }
  }
  
  /**
   * Get the long default value for the given field.
   * @param field the field
   * @return the default value
   */
  private static long longDefault(FieldDescriptor field) {
    return (Long) field.getDefaultValue();
  }
}
