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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Responsible to keep all constant keys related to ABFS metrics.
 */
@InterfaceAudience.Private
public final class MetricsConstants {
    public static final String COLON = ":";
    public static final String RETRY = "RETRY";
    public static final String BASE = "BASE";
    public static final String FILE = "FILE";

    // Private constructor to prevent instantiation
    private MetricsConstants() {
        throw new AssertionError("Cannot instantiate MetricsConstants");
    }
}
