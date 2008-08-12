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

	@Override
	public String toString()
	{
		Set <Map.Entry<String,Buffer>> f = this.mapFields.entrySet();
		Iterator <Map.Entry<String,Buffer>> it = f.iterator();
		
		Map.Entry<String,Buffer> entry = null;
		StringBuilder sb = new StringBuilder();
		sb.append("<event  ");
		String body = null;
		String key = null;
		String val = null;
		
		while (it.hasNext())
		{
			entry = it.next();
			key = entry.getKey().intern();
			val = new String(entry.getValue().get());
			if (key == Record.rawField.intern())
			{
				continue;
			}
			
			if (key == Record.bodyField.intern())
			{
				body = val;
			}
			else
			{
				sb.append(entry.getKey()).append("=\"").append(val).append("\" ");
			}
		}
		sb.append(">").append(body);
		sb.append("</event>");
		
		return sb.toString();
//		//<event start="Jun 15 2008 00:00:00" end="Jun 15 2008 12:00:00" title="hello" link="/here">body</event>
//		return 	"<event start=\"" + formatter.format(new Date(this.getTime())) + "\" title=\""
//		+  this.getValue(Record.sourceField)   + "\" >" + this.getValue(Record.bodyField) + "</event>" ;
	}

	
}
