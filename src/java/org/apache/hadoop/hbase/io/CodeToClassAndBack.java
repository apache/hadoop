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

package org.apache.hadoop.hbase.io;

import java.util.*;

/**
 * A Static Interface.
 * Instead of having this code in the the HbaseMapWritable code, where it
 * blocks the possibility of altering the variables and changing their types,
 * it is put here in this static interface where the static final Maps are
 * loaded one time. Only byte[] and Cell are supported at this time.
 */
public interface CodeToClassAndBack {
  /**
   * Static map that contains mapping from code to class
   */
  public static final Map<Byte, Class<?>> CODE_TO_CLASS =
    new HashMap<Byte, Class<?>>();

  /**
   * Static map that contains mapping from class to code
   */
  public static final Map<Class<?>, Byte> CLASS_TO_CODE =
    new HashMap<Class<?>, Byte>();
  
  /**
   * Class list for supported classes
   */
  public Class[] classList = {byte[].class, Cell.class};
  
  /**
   * The static loader that is used instead of the static constructor in
   * HbaseMapWritable.
   */
  public InternalStaticLoader sl =
    new InternalStaticLoader(classList, CODE_TO_CLASS, CLASS_TO_CODE);

  /**
   * Class that loads the static maps with their values. 
   */
  public class InternalStaticLoader{
    InternalStaticLoader(Class[] classList, Map<Byte, Class<?>> CODE_TO_CLASS,
    Map<Class<?>, Byte> CLASS_TO_CODE){
      byte code = 1;
      for(int i=0; i<classList.length; i++){
        CLASS_TO_CODE.put(classList[i], code);
        CODE_TO_CLASS.put(code, classList[i]);
        code++;
      }
    }
  }
}
