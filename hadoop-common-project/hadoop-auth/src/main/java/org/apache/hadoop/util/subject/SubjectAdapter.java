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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

/**
 * javax.security.auth.Subject.getSubject is deprecated for removal.
 * The replacement API exists only in Java 18 and above.
 * This class helps use the newer API if available, without raising the language level.
 */
public class SubjectAdapter {
    private static Logger log = LoggerFactory.getLogger(SubjectAdapter.class);
    private static final HiddenGetSubject instance;
    static {
        int version = 0;
        try {
            version = Integer.parseInt(System.getProperty("java.specification.version"));
        } catch (Throwable ignored) {}
        if (version >= 18) {
            instance = new GetSubjectJava18AndAbove();
        } else {
            instance = new ClassicGetSubject();
        }
    }

    private SubjectAdapter() {}

    public static Subject getSubject() {
        return instance.getSubject();
    }

    /**
     * This main method is included so that this is trivially tested using multiple JDKs outside the scope test sources
     * @param args ignored
     */
    public static void main(String[] args) {
        final Subject theSubject = getSubject();
        log.info("Current subject is {}", theSubject);
    }
}
