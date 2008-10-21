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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;

@explain(displayName="Fetch Operator")
public class fetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  //  private loadFileDesc loadFileWork;
  //  private tableDesc    tblDesc;
  private Path srcDir;
  private Properties schema;
  private Class<? extends Deserializer> deserializerClass;
  private Class<? extends InputFormat> inputFormatClass;
  private int limit;

  public fetchWork() { }

	/**
	 * @param deserializer
	 * @param deserializerClass
	 * @param inputFormatClass
	 * @param schema
	 * @param srcDir
	 */
	public fetchWork(Path srcDir,
			Class<? extends Deserializer> deserializerClass,
			Class<? extends InputFormat> inputFormatClass, Properties schema,
			int limit) {
		this.srcDir = srcDir;
		this.deserializerClass = deserializerClass;
		this.inputFormatClass = inputFormatClass;
		this.schema = schema;
		this.limit = limit;
	}

	/**
	 * @return the srcDir
	 */
  @explain(displayName="source")
	public Path getSrcDir() {
		return srcDir;
	}

	/**
	 * @param srcDir the srcDir to set
	 */
	public void setSrcDir(Path srcDir) {
		this.srcDir = srcDir;
	}

	/**
	 * @return the schema
	 */
	public Properties getSchema() {
		return schema;
	}

	/**
	 * @param schema the schema to set
	 */
	public void setSchema(Properties schema) {
		this.schema = schema;
	}

	/**
	 * @return the deserializerClass
	 */
	public Class<? extends Deserializer> getDeserializerClass() {
		return deserializerClass;
	}

	/**
	 * @param deserializerClass the deserializerClass to set
	 */
	public void setDeserializerClass(Class<? extends Deserializer> deserializerClass) {
		this.deserializerClass = deserializerClass;
	}

	/**
	 * @return the inputFormatClass
	 */
	public Class<? extends InputFormat> getInputFormatClass() {
		return inputFormatClass;
	}

	/**
	 * @param inputFormatClass the inputFormatClass to set
	 */
	public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	/**
	 * @return the limit
	 */
  @explain(displayName="limit")
	public int getLimit() {
		return limit;
	}

	/**
	 * @param limit the limit to set
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}
}
