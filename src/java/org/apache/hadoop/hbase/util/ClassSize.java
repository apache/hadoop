/**
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

package org.apache.hadoop.hbase.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HeapSize;

/**
 * Class for determining the "size" of a class, an attempt to calculate the
 * actual bytes that an object of this class will occupy in memory
 * 
 * The core of this class is taken from the Derby project
 */
public class ClassSize {
  static final Log LOG = LogFactory.getLog(ClassSize.class);
  
  private int refSize;
  private int minObjectSize;

  /**
   * Constructor
   * @throws Exception
   */
  public ClassSize() throws Exception{
    // Figure out whether this is a 32 or 64 bit machine.
    Runtime runtime = Runtime.getRuntime();
    int loops = 10;
    int sz = 0;
    for(int i = 0; i < loops; i++) {
      cleaner(runtime, i);
      long memBase = runtime.totalMemory() - runtime.freeMemory();  
      Object[] junk = new Object[10000];
      cleaner(runtime, i);
      long memUsed = runtime.totalMemory() - runtime.freeMemory() - memBase;
      sz = (int)((memUsed + junk.length/2)/junk.length);
      if(sz > 0 ) {
        break;
      }
    }

    refSize = ( 4 > sz) ? 4 : sz;
    minObjectSize = 4*refSize;
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
   * @return an array of 3 integers. The first integer is the size of the 
   * primitives, the second the number of arrays and the third the number of
   * references.
   */
  private int [] getSizeCoefficients(Class cl, boolean debug) {
    int primitives = 0;
    int arrays = 0;
    int references = HeapSize.OBJECT / HeapSize.REFERENCE;

    for( ; null != cl; cl = cl.getSuperclass()) {
      Field[] field = cl.getDeclaredFields();
      if( null != field) {
        for( int i = 0; i < field.length; i++) {
          if( ! Modifier.isStatic( field[i].getModifiers())) {
            Class fieldClass = field[i].getType();
            if( fieldClass.isArray()){
              arrays++;
            }
            else if(! fieldClass.isPrimitive()){
              references++;
            }
            else {// Is simple primitive
              String name = fieldClass.getName();

              if(name.equals("int") || name.equals( "I"))
                primitives += Bytes.SIZEOF_INT;
              else if(name.equals("long") || name.equals( "J"))
                primitives += Bytes.SIZEOF_LONG;
              else if(name.equals("boolean") || name.equals( "Z"))
                primitives += Bytes.SIZEOF_BOOLEAN;
              else if(name.equals("short") || name.equals( "S"))
                primitives += Bytes.SIZEOF_SHORT;
              else if(name.equals("byte") || name.equals( "B"))
                primitives += Bytes.SIZEOF_BYTE;
              else if(name.equals("char") || name.equals( "C"))
                primitives += Bytes.SIZEOF_CHAR;
              else if(name.equals("float") || name.equals( "F"))
                primitives += Bytes.SIZEOF_FLOAT;
              else if(name.equals("double") || name.equals( "D"))
                primitives += Bytes.SIZEOF_DOUBLE;
            }
            if(debug) {
              if (LOG.isDebugEnabled()) {
                // Write out region name as string and its encoded name.
                LOG.debug(field[i].getName()+ "\n\t" +field[i].getType());
              }
            }
          }
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
   * @return the size estimate, in bytes
   */
  private long estimateBaseFromCoefficients(int [] coeff, boolean debug) {
    int size = coeff[0] + (coeff[1]*4 + coeff[2])*refSize;

    // Round up to a multiple of 8
    size = (int)alignSize(size);
    if(debug) {
      if (LOG.isDebugEnabled()) {
        // Write out region name as string and its encoded name.
        LOG.debug("Primitives " + coeff[0] + ", arrays " + coeff[1] +
            ", references(inlcuding " + HeapSize.OBJECT + 
            ", for object overhead) " + coeff[2] + ", refSize " + refSize + 
            ", size " + size);
      }
    }
    return (size < minObjectSize) ? minObjectSize : size;
  } 

  /**
   * Estimate the static space taken up by the fields of a class. This includes 
   * the space taken up by by references (the pointer) but not by the referenced 
   * object. So the estimated size of an array field does not depend on the size 
   * of the array. Similarly the size of an object (reference) field does not 
   * depend on the object.
   *
   * @return the size estimate in bytes.
   */
  public long estimateBase(Class cl, boolean debug) {
    return estimateBaseFromCoefficients( getSizeCoefficients(cl, debug), debug);
  } 

  /**
   * Tries to clear all the memory used to estimate the reference size for the
   * current JVM
   * @param runtime
   * @param i
   * @throws Exception
   */
  private void cleaner(Runtime runtime, int i) throws Exception{
    Thread.sleep(i*1000);
    runtime.gc();runtime.gc(); runtime.gc();runtime.gc();runtime.gc();
    runtime.runFinalization();
  }

  
  /**
   * Aligns a number to 8.
   * @param num number to align to 8
   * @return smallest number >= input that is a multiple of 8
   */
  public static long alignSize(int num) {
    int aligned = (num + 7)/8;
    aligned *= 8;
    return aligned;
  }
  
}

