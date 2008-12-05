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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.record.Buffer;

public class ChukwaRecord extends ChukwaRecordJT
implements Record
{
	public ChukwaRecord()
	{}
	
	
	
	public void add(String key, String value)
	{
		synchronized(this)
		{
			if (this.mapFields == null)
			{
				this.mapFields = new TreeMap<String,org.apache.hadoop.record.Buffer>();
			}
		}
		this.mapFields.put(key, new Buffer(value.getBytes()));
	}
	
	public String[] getFields()
	{
		return this.mapFields.keySet().toArray(new String[0]);
	}

	public String getValue(String field)
	{
		if (this.mapFields.containsKey(field))
		{
			return new String(this.mapFields.get(field).get());		
		}
		else
		{
			return null;
		}
	}

	public boolean containsField(String field)
	{
		return this.mapFields.containsKey(field);
	}

	public void removeValue(String field) {
		if(this.mapFields.containsKey(field)) {
			this.mapFields.remove(field);
		}
	}
	
	@Override
	public String toString()
	{
		Set <Map.Entry<String,Buffer>> f = this.mapFields.entrySet();
		Iterator <Map.Entry<String,Buffer>> it = f.iterator();
		
		Map.Entry<String,Buffer> entry = null;
		StringBuilder sb = new StringBuilder();
		sb.append("<event  ");
		StringBuilder body = new StringBuilder();
		
		String key = null;
		String val = null;
		boolean hasBody = false;
		String bodyVal = null;
		while (it.hasNext())
		{
			entry = it.next();
			key = entry.getKey().intern();
			val = new String(entry.getValue().get());
			if (key.intern() == Record.bodyField.intern())
			{
				hasBody = true;
				bodyVal = val;
			}
			else
			{
				sb.append(key).append("=\"").append(val).append("\" ");
				body.append(key).append( " = ").append(val).append("<br>");
			}
			
			
		}
		if (hasBody)	
		{ sb.append(">").append(bodyVal);}
		else
		{ sb.append(">").append(body);}
		sb.append("</event>");
		
		return sb.toString();
	}

	
}
