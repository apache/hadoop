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

package org.apache.hadoop.chukwa.extraction.engine;

public interface Record
{
	public static final String bodyField = "body";
	
	public static final String logLevelField = "logLevel";
	public static final String destinationField = "dest";
	public static final String dataSourceField = "ds";
	public static final String sourceField = "src";
	public static final String streamNameField = "sname";
	public static final String typeField = "type";
	public static final String classField = "pkg";
	public static final String rawField = "raw";
	
	public static final String fieldSeparator = ":";
	
	public long getTime();
	public void add(String key, String value);
	public String[] getFields();
	public String getValue(String field);
	public String toString();
}
