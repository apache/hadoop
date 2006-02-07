/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.util.HashMap;

/** Factories for non-public writables.  Defining a factory permits {@link
 * ObjectWritable} to be able to construct instances of non-public classes. */
public class WritableFactories {
  private static final HashMap CLASS_TO_FACTORY = new HashMap();

  private WritableFactories() {}                  // singleton

  /** Define a factory for a class. */
  public static synchronized void setFactory(Class c, WritableFactory factory) {
    CLASS_TO_FACTORY.put(c, factory);
  }

  /** Define a factory for a class. */
  public static synchronized WritableFactory getFactory(Class c) {
    return (WritableFactory)CLASS_TO_FACTORY.get(c);
  }

  /** Create a new instance of a class with a defined factory. */
  public static Writable newInstance(Class c) {
    WritableFactory factory = WritableFactories.getFactory(c);
    if (factory != null) {
      return factory.newInstance();
    } else {
      try {
        return (Writable)c.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

}

