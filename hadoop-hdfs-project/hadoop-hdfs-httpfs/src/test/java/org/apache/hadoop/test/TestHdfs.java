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
package org.apache.hadoop.test;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;


/**
 * Annotation for {@link HTestCase} subclasses to indicate that the test method
 * requires a FileSystemAccess cluster.
 * <p/>
 * The {@link TestHdfsHelper#getHdfsConf()} returns a FileSystemAccess JobConf preconfigured to connect
 * to the FileSystemAccess test minicluster or the FileSystemAccess cluster information.
 * <p/>
 * A HDFS test directory for the test will be created. The HDFS test directory
 * location can be retrieve using the {@link TestHdfsHelper#getHdfsTestDir()} method.
 * <p/>
 * Refer to the {@link HTestCase} class for details on how to use and configure
 * a FileSystemAccess test minicluster or a real FileSystemAccess cluster for the tests.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(java.lang.annotation.ElementType.METHOD)
public @interface TestHdfs {
}
