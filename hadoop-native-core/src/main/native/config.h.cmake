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
#ifndef CONFIG_H
#define CONFIG_H

/**
 * Defined if we can use the __thread keyword to get faster thread-local 
 * storage.
 */
#cmakedefine HAVE_BETTER_TLS

/**
 * Short version of JNI library name.  
 * This varies by platform.
 *
 * Example: "libvjm.so"
 */
#cmakedefine JNI_LIBRARY_NAME "@JNI_LIBRARY_NAME@"

/**
 * Where to find the test XML files we use in hconf-unit.
 */
#cmakedefine HCONF_XML_TEST_PATH "@HCONF_XML_TEST_PATH@"

#endif
