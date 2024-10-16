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

package org.apache.hadoop.util.subject;

import javax.security.auth.Subject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Indirectly calls Subject.current(), which exists in Java 18 and above only
 */
class SubjectAdapterJava18AndAbove implements HiddenSubjectAdapter {
    private final Method currentMethod;

    SubjectAdapterJava18AndAbove() {
        try {
            currentMethod = Subject.class.getMethod("current");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to find javax.security.auth.Subject.current() method", e);
        }
    }

    @Override
    public Subject getSubject() {
        try {
            return (Subject) currentMethod.invoke(null);
        } catch (IllegalAccessException | InvocationTargetException e) {
            // we would return null, but null has meaning here
            throw new RuntimeException("Unable to call Subject.current()", e);
        }
    }
}
