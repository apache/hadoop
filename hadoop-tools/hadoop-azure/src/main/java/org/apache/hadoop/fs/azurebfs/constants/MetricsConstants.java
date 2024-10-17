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

import java.util.Arrays;
import java.util.List;

public final class MetricsConstants {
    public static final String COLON = ":";
    public static final String RETRY = "RETRY";
    public static final String BASE = "BASE";
    public static final List<String> RETRY_LIST = Arrays.asList("1", "2", "3", "4", "5_15", "15_25", "25AndAbove");
    public static final String COUNTER = "COUNTER";
    public static final String GAUGE = "GAUGE";
    public static final String PARQUET = "PARQUET";
    public static final String NON_PARQUET = "NON_PARQUET";
}
