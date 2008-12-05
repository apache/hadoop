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

package org.apache.hadoop.chukwa.extraction.demux.processor;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Util
{
	static SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd");
	
	static Calendar calendar = Calendar.getInstance();
	static int currentDay = 0;
	static int currentHour = 0;
	
	static
	{
		synchronized(calendar)
		{
			calendar.setTimeInMillis( System.currentTimeMillis());
			currentDay = Integer.parseInt(day.format(calendar.getTime()));
			currentHour = calendar.get(Calendar.HOUR_OF_DAY);
		}
	}
	
	public static String generateTimeOutput(long timestamp)
	{
		int workingDay = 0;
		int workingHour = 0;

		String output = null;

		int minutes = 0;
		synchronized(calendar)
		{
			calendar.setTimeInMillis( timestamp);
			workingDay = Integer.parseInt(day.format(calendar.getTime()));
			workingHour = calendar.get(Calendar.HOUR_OF_DAY);
			minutes = calendar.get(Calendar.MINUTE);
		}
		
		if (workingDay != currentDay)
		{
			output = "_" + workingDay + ".D.evt";
		}
		else
		{
			if (workingHour != currentHour)
			{
				output = "_" +workingDay + "_" +  workingHour + ".H.evt";
			}
			else
			{
				output = "_" + workingDay + "_" +  workingHour + "_";
				int dec = minutes/10;
				output +=  dec ;

				int m = minutes - (dec*10);
				if (m < 5) 
				{ 
					output += "0.R.evt";
				} 
				else 
				{
					output += "5.R.evt";
				}
			}
		}


		return output;
	}
}
