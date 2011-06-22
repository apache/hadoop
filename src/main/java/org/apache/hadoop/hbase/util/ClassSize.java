/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

/**
 * Class for determining the "size" of a class, an attempt to calculate the
 * actual bytes that an object of this class will occupy in memory
 *
 * The core of this class is taken from the Derby project
 */
public class ClassSize {
  static final Log LOG = LogFactory.getLog(ClassSize.class);

  private static int nrOfRefsPerObj = 2;

  /** Array overhead */
  public static final int ARRAY;

  /** Overhead for ArrayList(0) */
  public static final int ARRAYLIST;

  /** Overhead for ByteBuffer */
  public static final int BYTE_BUFFER;

  /** Overhead for an Integer */
  public static final int INTEGER;

  /** Overhead for entry in map */
  public static final int MAP_ENTRY;

  /** Object overhead is minimum 2 * reference size (8 bytes on 64-bit) */
  public static final int OBJECT;

  /** Reference size is 8 bytes on 64-bit, 4 bytes on 32-bit */
  public static final int REFERENCE;

  /** String overhead */
  public static final int STRING;

  /** Overhead for TreeMap */
  public static final int TREEMAP;

  /** Overhead for ConcurrentHashMap */
  public static final int CONCURRENT_HASHMAP;

  /** Overhead for ConcurrentHashMap.Entry */
  public static final int CONCURRENT_HASHMAP_ENTRY;

  /** Overhead for ConcurrentHashMap.Segment */
  public static final int CONCURRENT_HASHMAP_SEGMENT;

  /** Overhead for ConcurrentSkipListMap */
  public static final int CONCURRENT_SKIPLISTMAP;

  /** Overhead for ConcurrentSkipListMap Entry */
  public static final int CONCURRENT_SKIPLISTMAP_ENTRY;

  /** Overhead for ReentrantReadWriteLock */
  public static final int REENTRANT_LOCK;

  /** Overhead for AtomicLong */
  public static final int ATOMIC_LONG;

  /** Overhead for AtomicInteger */
  public static final int ATOMIC_INTEGER;

  /** Overhead for AtomicBoolean */
  public static final int ATOMIC_BOOLEAN;

  /** Overhead for CopyOnWriteArraySet */
  public static final int COPYONWRITE_ARRAYSET;

  /** Overhead for CopyOnWriteArrayList */
  public static final int COPYONWRITE_ARRAYLIST;

  private static final String THIRTY_TWO = "32";

  /**
   * Method for reading the arc settings and setting overheads according
   * to 32-bit or 64-bit architecture.
   */
  static {
    // Figure out whether this is a 32 or 64 bit machine.
    Properties sysProps = System.getProperties();
    String arcModel = sysProps.getProperty("sun.arch.data.model");

    //Default value is set to 8, covering the case when arcModel is unknown
    if (arcModel.equals(THIRTY_TWO)) {
      REFERENCE = 4;
    } else {
      REFERENCE = 8;
    }

    OBJECT = 2 * REFERENCE;

    ARRAY = align(3 * REFERENCE);

    ARRAYLIST = align(OBJECT + align(REFERENCE) + align(ARRAY) +
        (2 * Bytes.SIZEOF_INT));

    //noinspection PointlessArithmeticExpression
    BYTE_BUFFER = align(OBJECT + align(REFERENCE) + align(ARRAY) +
        (5 * Bytes.SIZEOF_INT) +
        (3 * Bytes.SIZEOF_BOOLEAN) + Bytes.SIZEOF_LONG);

    INTEGER = align(OBJECT + Bytes.SIZEOF_INT);

    MAP_ENTRY = align(OBJECT + 5 * REFERENCE + Bytes.SIZEOF_BOOLEAN);

    TREEMAP = align(OBJECT + (2 * Bytes.SIZEOF_INT) + align(7 * REFERENCE));

    STRING = align(OBJECT + ARRAY + REFERENCE + 3 * Bytes.SIZEOF_INT);

    CONCURRENT_HASHMAP = align((2 * Bytes.SIZEOF_INT) + ARRAY +
        (6 * REFERENCE) + OBJECT);

    CONCURRENT_HASHMAP_ENTRY = align(REFERENCE + OBJECT + (3 * REFERENCE) +
        (2 * Bytes.SIZEOF_INT));

    CONCURRENT_HASHMAP_SEGMENT = align(REFERENCE + OBJECT +
        (3 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_FLOAT + ARRAY);

    CONCURRENT_SKIPLISTMAP = align(Bytes.SIZEOF_INT + OBJECT + (8 * REFERENCE));

    CONCURRENT_SKIPLISTMAP_ENTRY = align(
        align(OBJECT + (3 * REFERENCE)) + /* one node per entry */
        align((OBJECT + (3 * REFERENCE))/2)); /* one index per two entries */

    REENTRANT_LOCK = align(OBJECT + (3 * REFERENCE));

    ATOMIC_LONG = align(OBJECT + Bytes.SIZEOF_LONG);

    ATOMIC_INTEGER = align(OBJECT + Bytes.SIZEOF_INT);

    ATOMIC_BOOLEAN = align(OBJECT + Bytes.SIZEOF_BOOLEAN);

    COPYONWRITE_ARRAYSET = align(OBJECT + REFERENCE);

    COPYONWRITE_ARRAYLIST = align(OBJECT + (2 * REFERENCE) + ARRAY);
  }

  /**
   * The estimate of the size of a class instance depends on whether the JVM
   * uses 32 or 64 bit addresses, that is it depends on the size of an object
   * reference. It is a linear function of the size of a reference, e.g.
   * 24 + 5*r where r is the size of a reference (usually 4 or 8 bytes).
   *
   * This method returns the coefficients of the linear function, e.g. {24, 5}
   * in the above example.
   *
   * @param cl A class whose instance size is to be estimated
   * @param debug debug flag
   * @return an array of 3 integers. The first integer is the size of the
   * primitives, the second the number of arrays and the third the number of
   * references.
   */
  @SuppressWarnings("unchecked")
  private static int [] getSizeCoefficients(Class cl, boolean debug) {
    int primitives = 0;
    int arrays = 0;
    //The number of references that a new object takes
    int references = nrOfRefsPerObj;
    int index = 0;

    for ( ; null != cl; cl = cl.getSuperclass()) {
      Field[] field = cl.getDeclaredFields();
      if (null != field) {
        for (Field aField : field) {
          if (Modifier.isStatic(aField.getModifiers())) continue;
          Class fieldClass = aField.getType();
          if (fieldClass.isArray()) {
            arrays++;
            references++;
          } else if (!fieldClass.isPrimitive()) {
            references++;
          } else {// Is simple primitive
            String name = fieldClass.getName();

            if (name.equals("int") || name.equals("I"))
              primitives += Bytes.SIZEOF_INT;
            else if (name.equals("long") || name.equals("J"))
              primitives += Bytes.SIZEOF_LONG;
            else if (name.equals("boolean") || name.equals("Z"))
              primitives += Bytes.SIZEOF_BOOLEAN;
            else if (name.equals("short") || name.equals("S"))
              primitives += Bytes.SIZEOF_SHORT;
            else if (name.equals("byte") || name.equals("B"))
              primitives += Bytes.SIZEOF_BYTE;
            else if (name.equals("char") || name.equals("C"))
              primitives += Bytes.SIZEOF_CHAR;
            else if (name.equals("float") || name.equals("F"))
              primitives += Bytes.SIZEOF_FLOAT;
            else if (name.equals("double") || name.equals("D"))
              primitives += Bytes.SIZEOF_DOUBLE;
          }
          if (debug) {
            if (LOG.isDebugEnabled()) {
              // Write out region name as string and its encoded name.
              LOG.debug("" + index + " " + aField.getName() + " " + aField.getType());
            }
          }
          index++;
        }
      }
    }
    return new int [] {primitives, arrays, references};
  }

  /**
   * Estimate the static space taken up by a class instance given the
   * coefficients returned by getSizeCoefficients.
   *
   * @param coeff the coefficients
   *
   * @param debug debug flag
   * @return the size estimate, in bytes
   */
  private static long estimateBaseFromCoefficients(int [] coeff, boolean debug) {
    long prealign_size = coeff[0] + align(coeff[1] * ARRAY) + coeff[2] * REFERENCE;

    // Round up to a multiple of 8
    long size = align(prealign_size);
    if(debug) {
      if (LOG.isDebugEnabled()) {
        // Write out region name as string and its encoded name.
        LOG.debug("Primitives=" + coeff[0] + ", arrays=" + coeff[1] +
            ", references(includes " + nrOfRefsPerObj +
            " for object overhead)=" + coeff[2] + ", refSize " + REFERENCE +
            ", size=" + size + ", prealign_size=" + prealign_size);
      }
    }
    return size;
  }

  /**
   * Estimate the static space taken up by the fields of a class. This includes
   * the space taken up by by references (the pointer) but not by the referenced
   * object. So the estimated size of an array field does not depend on the size
   * of the array. Similarly the size of an object (reference) field does not
   * depend on the object.
   *
   * @param cl class
   * @param debug debug flag
   * @return the size estimate in bytes.
   */
  @SuppressWarnings("unchecked")
  public static long estimateBase(Class cl, boolean debug) {
    return estimateBaseFromCoefficients( getSizeCoefficients(cl, debug), debug);
  }

  /**
   * Aligns a number to 8.
   * @param num number to align to 8
   * @return smallest number >= input that is a multiple of 8
   */
  public static int align(int num) {
    return (int)(align((long)num));
  }

  /**
   * Aligns a number to 8.
   * @param num number to align to 8
   * @return smallest number >= input that is a multiple of 8
   */
  public static long align(long num) {
    //The 7 comes from that the alignSize is 8 which is the number of bytes
    //stored and sent together
    return  ((num + 7) >> 3) << 3;
  }
}

