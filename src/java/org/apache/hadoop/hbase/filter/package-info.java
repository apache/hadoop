/*
 * Copyright 2008 The Apache Software Foundation
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
/**Provides row-level filters applied to HRegion scan results during calls to {@link org.apache.hadoop.hbase.client.ResultScanner#next()}. 

<p>Use {@link org.apache.hadoop.hbase.filter.StopRowFilter} to stop the scan once rows exceed the supplied row key.
Filters will not stop the scan unless hosted inside of a {@link org.apache.hadoop.hbase.filter.WhileMatchRowFilter}.
Supply a set of filters to apply using {@link org.apache.hadoop.hbase.filter.RowFilterSet}.  
</p>
*/
package org.apache.hadoop.hbase.filter;
