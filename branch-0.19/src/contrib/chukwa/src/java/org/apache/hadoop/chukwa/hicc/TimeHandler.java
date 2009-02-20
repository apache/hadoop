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

package org.apache.hadoop.chukwa.hicc;

import javax.servlet.http.*;
import java.util.Calendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

public class TimeHandler {
	private HttpSession session = null;
	private HttpServletRequest request = null;
	private TimeZone tz = null;
    private long start = 0;
    private long end = 0;
    private String startDate = null;
    private String startHour = null;
    private String startMin = null;
    private String endDate = null;
    private String endHour = null;
    private String endMin = null;
    private String startS = null;
    private String endS = null;
    
    public TimeHandler(HttpServletRequest request) {
    	this.tz=TimeZone.getTimeZone("UTC");
    	init(request);
    }
    
    public TimeHandler(HttpServletRequest request, String tz) {
    	this.tz=TimeZone.getTimeZone(tz);
    	init(request);
    }
    
    public void init(HttpServletRequest request) {
    	this.session = request.getSession();
    	this.request = request;
    	String timeType = (String)session.getAttribute("time_type");
    	if((request.getParameter("period")!=null && !request.getParameter("period").equals("")) || (timeType!=null && timeType.equals("last"))) {
            String period = request.getParameter("period");
        	if(period == null) {
                period = (String) session.getAttribute("period");
                if(period == null) {
                    period = "last1hr";
                    session.setAttribute("period",period);
                }
            }
            Calendar now = Calendar.getInstance();
            this.start = now.getTimeInMillis();
            this.end = now.getTimeInMillis();            
        	if(period.equals("last1hr")) {
                start = end - (60*60*1000);
            } else if(period.equals("last2hr")) {
                start = end - (2*60*60*1000);
            } else if(period.equals("last3hr")) {
                start = end - (3*60*60*1000);
            } else if(period.equals("last6hr")) {
                start = end - (6*60*60*1000);
            } else if(period.equals("last12hr")) {
                start = end - (12*60*60*1000);
            } else if(period.equals("last24hr")) {
                start = end - (24*60*60*1000);
            } else if(period.equals("last7d")) {
                start = end - (7*24*60*60*1000);
            } else if(period.equals("last30d")) {
                start = end - (30*24*60*60*1000);
            }
        	if(request.getParameter("time_type")!=null && request.getParameter("time_type").equals("range")) {
                session.setAttribute("start", ""+start);
                session.setAttribute("end", ""+end);
        	}
        } else {
            start = Long.parseLong((String) session.getAttribute("start"));
            end = Long.parseLong((String) session.getAttribute("end"));        	
        }
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat formatHour = new SimpleDateFormat("HH");
        SimpleDateFormat formatMin = new SimpleDateFormat("mm");
        
        formatter.setTimeZone(this.tz);
        formatDate.setTimeZone(this.tz);
        formatHour.setTimeZone(this.tz);
        formatMin.setTimeZone(this.tz);
        
        startS = formatter.format(start);
        this.startDate = formatDate.format(start);
        this.startHour = formatHour.format(start);
        this.startMin = formatMin.format(start);
        endS = formatter.format(end);
        this.endDate = formatDate.format(end);
        this.endHour = formatHour.format(end);
        this.endMin = formatMin.format(end);
    }

    public String getStartDate() {
        return this.startDate;        	
    }
        
    public String getStartHour() {
        return this.startHour;
    }

    public String getStartMinute() {
        return this.startMin;
    }

    public String getStartTimeText() {
    	return this.startS;
    }
    
    public long getStartTime() {
        return start;	
    }
    
    public String getEndDate() {
    	return this.endDate;
    }

    public String getEndHour() {
        return this.endHour;
    }

    public String getEndMinute() {
        return this.endMin;
    }
    
    public String getEndTimeText() {
    	return this.endS;
    }

    public long getEndTime() {
        return end;	
    }

}
