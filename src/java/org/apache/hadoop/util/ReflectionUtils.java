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

package org.apache.hadoop.util;

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

/**
 * General reflection utils
 */

public class ReflectionUtils {
    
    private static final Class[] emptyArray = new Class[]{};

    /** Create an object for the given class and initialize it from conf
     * 
     * @param theClass class of which an object is created
     * @param conf Configuration
     * @return a new object
     */
    public static Object newInstance(Class theClass, Configuration conf) {
        Object result;
        try {
            Constructor meth = theClass.getDeclaredConstructor(emptyArray);
            meth.setAccessible(true);
            result = meth.newInstance(emptyArray);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (conf != null) {
            if (result instanceof Configurable) {
                ((Configurable) result).setConf(conf);
            }
            if (conf instanceof JobConf && 
                    result instanceof JobConfigurable) {
                ((JobConfigurable)result).configure((JobConf) conf);
            }
        }
        return result;
    }
}
