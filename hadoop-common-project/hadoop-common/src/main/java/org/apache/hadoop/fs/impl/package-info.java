/*
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

/**
 * This package contains implementation classes for use inside
 * filesystems.
 *
 * These classes MUST NOT be directly exposed as the arguments
 * or return values of methods, or as part of a visible
 * inheritance tree.
 *
 * These classes MAY be returned behind interfaces.
 * When such interfaces are used as parameters, the methods
 * which accept the interfaces MUST NOT cast them to the classes
 * contained therein: they MUST interact purely through
 * the interface.
 *
 * That is: don't expose the implementation classes in here,
 * and don't expect input interface implementations to always
 * be the classes in here.
 *
 * These classes are for the private use of FileSystem/
 * FileContext implementations.
 * Implementation classes not developed within the ASF Hadoop
 * codebase MAY use these, with the caveat that these classes
 * are highly unstable.
 */

@InterfaceAudience.LimitedPrivate("Filesystems")
@InterfaceStability.Unstable
package org.apache.hadoop.fs.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
