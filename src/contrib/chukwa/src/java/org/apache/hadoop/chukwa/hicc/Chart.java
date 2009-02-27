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

import java.io.PrintWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.chukwa.hicc.ColorPicker;


public class Chart {
    private String id;
    private String title;
    private String graphType;
    private ArrayList<TreeMap<String, TreeMap<String, Double>>> dataset;
    private ArrayList<String> chartType;
	private boolean xLabelOn;
	private boolean yLabelOn;
	private boolean yRightLabelOn;
	private int width;
	private int height;
	private List<String> xLabelRange;
	private HashMap<String, Long> xLabelRangeHash;
	private HttpServletRequest request = null;
    private boolean legend;
    private String xLabel="";
    private String yLabel="";
    private String yRightLabel="";
    private int datasetCounter=0;
    private double max=0;
    private int seriesCounter=0;
    private List<String> rightList;
    private boolean userDefinedMax = false;
    private String[] seriesOrder=null;
    
	public Chart(HttpServletRequest request) {
		if(request.getParameter("boxId")!=null) {
			this.id=request.getParameter("boxId");
		} else {
			this.id="0";
		}
        this.title="Untitled Chart";
        this.graphType="image";
        this.xLabelOn=true;
        this.yLabelOn=true;
        this.width=400;
        this.height=200;
        this.request=request;
        this.legend=true;
        this.max=0;
        this.datasetCounter=0;
        this.seriesCounter=0;
        this.rightList = new ArrayList<String>();
        this.userDefinedMax=false;
        this.seriesOrder=null;
    }
	
	public void setYMax(double max) {
		this.max=max;
		this.userDefinedMax=true;
	}
	
	public void setSize(int width, int height) {
		this.width=width;
		this.height=height;
	}
    public void setGraphType(String graphType) {
    	if(graphType!=null) {
            this.graphType = graphType;
    	}
    }
    
    public void setTitle(String title) {
    	this.title=title;    	
    }
    
    public void setId(String id) {
    	this.id=id;
    }
    
    public void setDataSet(String chartType, TreeMap<String, TreeMap<String, Double>> data) {
    	if(this.dataset==null) {
    		this.dataset = new ArrayList<TreeMap<String, TreeMap<String, Double>>>();
    		this.chartType = new ArrayList<String>();
    	}
   		this.dataset.add(data);
   		this.chartType.add(chartType);
    }

	public void setSeriesOrder(String[] metrics) {
		this.seriesOrder = metrics;
	}

    public void setXAxisLabels(boolean toggle) {
    	xLabelOn = toggle;
    }

    public void setYAxisLabels(boolean toggle) {
    	yLabelOn = toggle;    	
    }

    public void setYAxisRightLabels(boolean toggle) {
    	yRightLabelOn = toggle;    	
    }

    public void setXAxisLabel(String label) {
    	xLabel = label;
    }

    public void setYAxisLabel(String label) {
    	yLabel = label;
    }

    public void setYAxisRightLabel(String label) {
    	yRightLabel = label;
    }

    public void setXLabelsRange(List<String> range) {
    	xLabelRange = range;
        xLabelRangeHash = new HashMap<String, Long>();
        long value = 0;
        for(String label : range) {
            xLabelRangeHash.put(label,value);
            value++;
        }
    }

    public void setLegend(boolean toggle) {
    	legend = toggle;
    }
    public String plot() {
    	String output="";
    	if(dataset==null) {
    		output = "No Data available.";
    		return output;
    	}
		String dateFormat="%H:%M";
		if(xLabel.equals("Time")) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        long xMin;
			try {
				xMin = Long.parseLong(xLabelRange.get(0));
    	        long xMax = Long.parseLong(xLabelRange.get(xLabelRange.size()-1));
    	        if(xMax-xMin>31536000000L) {
    	        	dateFormat="%y";
    	        } else if(xMax-xMin>2592000000L) {
    	        	dateFormat="%y-%m";
    	        } else if(xMax-xMin>604800000L) {
    	        	dateFormat="%m-%d";
    	        } else if(xMax-xMin>86400000L) {
    	        	dateFormat="%m-%d %H:%M";
    	        }
			} catch (NumberFormatException e) {
				dateFormat="%y-%m-%d %H:%M";
			}
		}
        String xAxisOptions = "";
        if(xLabel.equals("Time")) {
                xAxisOptions = "timeformat: \""+dateFormat+"\",mode: \"time\"";
        } else {
                xAxisOptions = "tickFormatter: function (val, axis) { return xLabels[Math.round(val)]; }, ticks: 0";
        }
        output = "<html><link href=\"/hicc/css/default.css\" rel=\"stylesheet\" type=\"text/css\">\n" +
		         "<body onresize=\"wholePeriod()\"><script type=\"text/javascript\" src=\"/hicc/js/jquery-1.2.6.min.js\"></script>\n"+
		         "<script type=\"text/javascript\" src=\"/hicc/js/jquery.flot.pack.js\"></script>\n"+
		         "<script type=\"text/javascript\" src=\"/hicc/js/excanvas.pack.js\"></script>\n"+
		         "<center>"+title+"</center>\n"+
		         "<div id=\"placeholder\" style=\"width:"+this.width+"px;height:"+this.height+"px;\"></div>\n"+
		         "<div id=\"placeholderLegend\"></div>\n"+
		         "<input type=\"hidden\" id=\"boxId\" value=\"iframe"+this.id+"\">\n"+
		         "<script type=\"text/javascript\" src=\"/hicc/js/flot.extend.js\">\n" +
		         "</script>\n" +
		         "<script type=\"text/javascript\">\n"+
                         "var xLabels=new Array();\n"+
		         "var cw = document.body.clientWidth-70;\n"+
		         "var ch = document.body.clientHeight-50;\n"+
		         "document.getElementById('placeholder').style.width=cw+'px';\n"+
		         "document.getElementById('placeholder').style.height=ch+'px';\n"+
		         "_options={\n"+
		         "        points: { show: false },\n"+
		         "        xaxis: { "+xAxisOptions+" },\n"+
		         "	  selection: { mode: \"x\" },\n"+
		         "	  grid: {\n"+
		         "	           clickable: true,\n"+
		         "	           hoverable: true,\n"+
		         "	           tickColor: \"#C0C0C0\",\n"+
		         "	           backgroundColor:\"#FFFFFF\"\n"+
		         "	  },\n"+
		         "	  legend: { show: "+this.legend+" },\n"+
                         "        yaxis: { ";
        boolean stack = false;
        for(String type : this.chartType) {
            if(type.startsWith("stack")) {
                stack=true;
            }
        }
        if(stack) {
            output = output + "mode: \"stack\", ";
        }
        if(userDefinedMax) {
        	output = output + "tickFormatter: function(val, axis) { " +
        	    "return val.toFixed(axis.tickDecimals) + \" %\"; }";
        } else {
            output = output + "tickFormatter: function(val, axis) { " +
		        "if (val > 1000000000000000) return (val / 1000000000000000).toFixed(axis.tickDecimals) + \"PB\";" +
                "else if (val > 1000000000000) return (val / 1000000000000).toFixed(axis.tickDecimals) + \"TB\";" +
				"else if (val > 1000000000) return (val / 1000000000).toFixed(axis.tickDecimals) + \"GB\";" +
        		"else if (val > 1000000) return (val / 1000000).toFixed(axis.tickDecimals) + \"MB\";" +
        		"else if (val > 1000) return (val / 1000).toFixed(axis.tickDecimals) + \"KB\";" +
        		"else return val.toFixed(axis.tickDecimals) + \"B\"; }";
        }
        if(userDefinedMax) {
            output = output + ", min:0, max:"+this.max;
        }
        output = output + "}\n";
        output = output + "	};\n";
        if(!xLabel.equals("Time")) {
            for(int i=0;i<xLabelRange.size();i++) {
                output = output + "xLabels[" + i + "]=\"" + xLabelRange.get(i)+"\";\n";
            }
        }
        output = output + "_series=[\n";
/*		            ArrayList<Long> numericLabelRange = new ArrayList<Long>();
				    if(xLabel.equals("Time")) {
				        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				        for(int i=0;i<xLabelRange.size();i++) {
				        	try {
				                Date d = formatter.parse(xLabelRange.get(i));
				                numericLabelRange.add(d.getTime());
				        	} catch(Exception e) {
				        	}
				        }                
				    } else {
					    for(int i=0;i<xLabelRange.size();i++) {
					    	numericLabelRange.add(Long.parseLong(xLabelRange.get(i)));
					    }
				    }*/
			        ColorPicker cp = new ColorPicker();
		   			int i=0;
				    for(TreeMap<String, TreeMap<String, Double>> dataMap : this.dataset) {
		   	            String[] keyNames = null;
		   	            if(this.seriesOrder!=null) {
		   	            	keyNames = this.seriesOrder;
		   	            } else {
    		   	            keyNames = ((String[]) dataMap.keySet().toArray(new String[dataMap.size()]));
		   	            }
			   			int counter=0;
			   			if(i!=0) {
			   				if(!this.userDefinedMax) {
			   				    this.max=0;
			   				}
			   			}
			   			for(String ki : keyNames) {
			   				TreeMap<String, Double> data = dataMap.get(ki);
			   				if(data!=null) {
			   				    for(String dp: data.keySet()) {
			   				    	try {
			   					        if(data.get(dp)>this.max) {
			   						        if(!this.userDefinedMax) {
			   						            this.max=data.get(dp);
			   						        }
			   					        }
			   				    	} catch (NullPointerException e) {
			   				    		// skip if this data point does not exist.
			   				    	}
			   				    }
			   				}
			   			}    			   			
		   	            for(String seriesName : keyNames) {
		   	   			    int counter2=0;
					 	    if(counter!=0) {
		   					    output+=",";
		   				    }
					 	    String param="fill: false";
					 	    String type="lines";
					 	    if(this.chartType.get(i).equals("stack-area") || this.chartType.get(i).equals("area")) {
					 	    	param="fill: true";
					 	    }
					 	    if(this.chartType.get(i).equals("bar")) {
					 	    	type="bars";
					 	    	param="stepByStep: true";
					 	    }
                                                    if(this.chartType.get(i).equals("point")) {
                                                        type="points";
                                                        param="fill: false";
                                                    }
					 	    output+="  {"+type+": { show: true, "+param+" }, color: \""+cp.get(counter+1)+"\", label: \""+seriesName+"\", ";
					 	    String showYAxis="false";
					 	    String shortRow="false";
					 	    if(counter==0 || i>0) {
					 	    	showYAxis="true";
					 	    	shortRow="false";
					 	    }
					 	    output+=" row: { show: "+showYAxis+",shortRow:"+shortRow+", showYAxis:"+showYAxis+"}, data:[";
		   	        	    TreeMap<String, Double> data = dataMap.get(seriesName);
		   	        	    for(String dp : data.keySet()) {
		   			 	        if(counter2!=0) {
		   					        output+=",";
		   				        }
                                                        if(xLabel.equals("Time")) {
		   				            output+="[\""+dp+"\","+data.get(dp)+"]";
                                                        } else {
                                                            long value = xLabelRangeHash.get(dp);
		   				            output+="[\""+value+"\","+data.get(dp)+"]";
                                                        }
		   				        counter2++;
		   			        }
		   	   			    output+="], min:0, max:"+this.max+"}";
		   	   			    counter++;
		   	            }
		   	            i++;
		    	    }          
		output+=" ];\n"+
		         " wholePeriod();</script></body></html>\n";
    	return output;
    }            
}
