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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class Df extends AbstractProcessor
{
	static Logger log = Logger.getLogger(Df.class);
	private static final String[] headerSplitCols = { "Filesystem", "1K-blocks",
			"Used", "Available", "Use%", "Mounted", "on" };
	private static final String[] headerCols = { "Filesystem", "1K-blocks",
    "Used", "Available", "Use%", "Mounted on" };
	private SimpleDateFormat sdf = null;

	public Df()
	{
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	}

	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter)
	throws Throwable
	{
	  
		try
		{
			String dStr = recordEntry.substring(0, 23);
			int start = 24;
			int idx = recordEntry.indexOf(' ', start);
			// String level = recordEntry.substring(start, idx);
			start = idx + 1;
			idx = recordEntry.indexOf(' ', start);
			// String className = recordEntry.substring(start, idx-1);
			String body = recordEntry.substring(idx + 1);

			Date d = sdf.parse(dStr);
			String[] lines = body.split("\n");

			String[] outputCols = lines[0].split("[\\s]++");
			
			if (outputCols.length != headerSplitCols.length
					|| outputCols[0].intern() != headerSplitCols[0].intern()
					|| outputCols[1].intern() != headerSplitCols[1].intern()
					|| outputCols[2].intern() != headerSplitCols[2].intern()
					|| outputCols[3].intern() != headerSplitCols[3].intern()
					|| outputCols[4].intern() != headerSplitCols[4].intern()
					|| outputCols[5].intern() != headerSplitCols[5].intern()
			    || outputCols[6].intern() != headerSplitCols[6].intern()
			    )
			{
			  throw new DFInvalidRecord("Wrong output format (header) ["
						+ recordEntry + "]");
			}

			String[] values = null;

			// Data
			ChukwaRecord record = null;

			for (int i = 1; i < lines.length; i++)
			{
				values = lines[i].split("[\\s]++");
				key = new ChukwaRecordKey();
				record = new ChukwaRecord();
				this.buildGenericRecord(record, null, d.getTime(), "Df");

				record.add(headerCols[0], values[0]);
				record.add(headerCols[1], values[1]);
				record.add(headerCols[2], values[2]);
				record.add(headerCols[3], values[3]);
				record.add(headerCols[4], values[4].substring(0, values[4].length()-1)); // Remove %
				record.add(headerCols[5], values[5]);

				output.collect(key, record);
			}

			//log.info("DFProcessor output 1 DF record");
		} catch (ParseException e)
		{
			e.printStackTrace();
			log.warn("Wrong format in DFProcessor [" + recordEntry + "]", e);
			throw e;
		} catch (IOException e)
		{
			e.printStackTrace();
			log.warn("Unable to collect output in DFProcessor [" + recordEntry
					+ "]", e);
			throw e;
		} catch (DFInvalidRecord e)
		{
			e.printStackTrace();
			log.warn("Wrong format in DFProcessor [" + recordEntry + "]", e);
			throw e;
		}
	}
}
