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

package org.apache.hadoop.chukwa.extraction.engine.datasource.record;

import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSourceException;

public class MalformedFileFormat extends DataSourceException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2180898410952691571L;

	public MalformedFileFormat()
	{
		super();
	}

	public MalformedFileFormat(String message, Throwable cause)
	{
		super(message, cause);
	}

	public MalformedFileFormat(String message)
	{
		super(message);
	}

	public MalformedFileFormat(Throwable cause)
	{
		super(cause);
	}

}
