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
		if(request!=null && request.getParameter("boxId")!=null) {
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
        SimpleDateFormat format = new SimpleDateFormat("m:s:S");
    	StringBuilder output= new StringBuilder();
    	if(dataset==null) {
    		output.append("No Data available.");
    		return output.toString();
        }
        String dateFormat="%H:%M";
        if(xLabel.intern()=="Time".intern()) {
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
        StringBuilder xAxisOptions = new StringBuilder();
        if(xLabel.intern()=="Time".intern()) {
            xAxisOptions.append("timeformat: \"");
            xAxisOptions.append(dateFormat);
            xAxisOptions.append("\",mode: \"time\"");
        } else {
            xAxisOptions.append("tickFormatter: function (val, axis) { return xLabels[Math.round(val)]; }, ticks: 0");
        }
        if(request!=null && request.getParameter("format")==null) {
            output.append("<html><link href=\"/hicc/css/default.css\" rel=\"stylesheet\" type=\"text/css\">\n");
            output.append("<body onresize=\"wholePeriod()\"><script type=\"text/javascript\" src=\"/hicc/js/jquery-1.2.6.min.js\"></script>\n");
            output.append("<script type=\"text/javascript\" src=\"/hicc/js/jquery.flot.pack.js\"></script>\n");
            output.append("<script type=\"text/javascript\" src=\"/hicc/js/excanvas.pack.js\"></script>\n");
            output.append("<div id=\"placeholderTitle\"><center>"+title+"</center></div>\n");
            output.append("<div id=\"placeholder\" style=\"width:"+this.width+"px;height:"+this.height+"px;\"></div>\n");
            output.append("<center><div id=\"placeholderLegend\"></div></center>\n");
            output.append("<input type=\"hidden\" id=\"boxId\" value=\"iframe"+this.id+"\">\n");
            output.append("<script type=\"text/javascript\" src=\"/hicc/js/flot.extend.js\">\n");
            output.append("</script>\n");
            output.append("<script type=\"text/javascript\">\n");
            output.append("var chartTitle=\"<center>"+title+"</center>\";\n");
            output.append("var height="+this.height+";\n");
            output.append("var xLabels=new Array();\n");
            output.append("var cw = document.body.clientWidth-70;\n");
            output.append("var ch = document.body.clientHeight-50;\n");
            output.append("document.getElementById('placeholder').style.width=cw+'px';\n");
            output.append("document.getElementById('placeholder').style.height=ch+'px';\n");
        }
        output.append("_options={\n");
        output.append("        points: { show: false },\n");
        output.append("        xaxis: { "+xAxisOptions+" },\n");
        output.append("	  selection: { mode: \"x\" },\n");
        output.append("	  grid: {\n");
        output.append("	           clickable: true,\n");
        output.append("	           hoverable: true,\n");
        output.append("	           tickColor: \"#C0C0C0\",\n");
        output.append("	           backgroundColor:\"#FFFFFF\"\n");
        output.append("	  },\n");
        output.append("	  legend: { show: "+this.legend+", noColumns: 3, container: $(\"#placeholderLegend\") },\n");
        output.append("        yaxis: { ");
        boolean stack = false;
        for(String type : this.chartType) {
            if(type.startsWith("stack")) {
                stack=true;
            }
        }
        if(stack) {
            output.append("mode: \"stack\", ");
        }
        if(userDefinedMax) {
            output.append("tickFormatter: function(val, axis) { return val.toFixed(axis.tickDecimals) + \" %\"; }");
        } else {
            output.append("tickFormatter: function(val, axis) { ");
            output.append("if (val >= 1000000000000000) return (val / 1000000000000000).toFixed(2) + \"x10<sup>15</sup>\";");
            output.append("else if (val >= 100000000000000) return (val / 100000000000000).toFixed(2) + \"x10<sup>14</sup>\";");
            output.append("else if (val >= 10000000000000) return (val / 10000000000000).toFixed(2) + \"x10<sup>13</sup>\";");
            output.append("else if (val >= 1000000000000) return (val / 1000000000000).toFixed(2) + \"x10<sup>12</sup>\";");
            output.append("else if (val >= 100000000000) return (val / 100000000000).toFixed(2) + \"x10<sup>11</sup>\";");
            output.append("else if (val >= 10000000000) return (val / 10000000000).toFixed(2) + \"x10<sup>10</sup>\";");
            output.append("else if (val >= 1000000000) return (val / 1000000000).toFixed(2) + \"x10<sup>9</sup>\";");
            output.append("else if (val >= 100000000) return (val / 100000000).toFixed(2) + \"x10<sup>8</sup>\";");
            output.append("else if (val >= 10000000) return (val / 10000000).toFixed(2) + \"x10<sup>7</sup>\";");
            output.append("else if (val >= 1000000) return (val / 1000000).toFixed(2) + \"x10<sup>6</sup>\";");
            output.append("else if (val >= 100000) return (val / 100000).toFixed(2) + \"x10<sup>5</sup>\";");
            output.append("else if (val >= 10000) return (val / 10000).toFixed(2) + \"x10<sup>4</sup>\";");
            output.append("else if (val >= 2000) return (val / 1000).toFixed(2) + \"x10<sup>3</sup>\";");
            output.append("else return val.toFixed(2) + \"\"; }");
        }
        if(userDefinedMax) {
            output.append(", min:0, max:");
            output.append(this.max);
        }
        output.append("}\n");
        output.append("	};\n");
        if(!xLabel.equals("Time")) {
            output.append("xLabels = [\"");
            for(int i=0;i<xLabelRange.size();i++) {
                if(i>0) {
                    output.append("\",\"");
                }
                output.append(xLabelRange.get(i));
            }
            output.append("\"];\n");
        }
        output.append("_series=[\n");
        ColorPicker cp = new ColorPicker();
        int i=0;
        for(TreeMap<String, TreeMap<String, Double>> dataMap : this.dataset) {
		   	String[] keyNames = null;
            if (this.seriesOrder != null) {
                keyNames = this.seriesOrder;
            } else {
                keyNames = ((String[]) dataMap.keySet().toArray(
                        new String[dataMap.size()]));
            }
            int counter = 0;
            if (i != 0) {
                if (!this.userDefinedMax) {
                    this.max = 0;
                }
            }
            for (String seriesName : keyNames) {
                int counter2 = 0;
                if (counter != 0) {
                    output.append(",");
                }
                String param = "fill: false";
                String type = "lines";
                if (this.chartType.get(i).intern() == "stack-area".intern()
                        || this.chartType.get(i).intern() == "area".intern()) {
                    param = "fill: true";
                }
                if (this.chartType.get(i).intern() == "bar".intern()) {
                    type = "bars";
                    param = "stepByStep: true";
                }
                if (this.chartType.get(i).intern() == "point".intern()) {
                    type = "points";
                    param = "fill: false";
                }
                output.append("  {");
                output.append(type);
                output.append(": { show: true, ");
                output.append(param);
                output.append(" }, color: \"");
                output.append(cp.get(counter + 1));
                output.append("\", label: \"");
                output.append(seriesName);
                output.append("\", ");
                String showYAxis = "false";
                String shortRow = "false";
                if (counter == 0 || i > 0) {
                    showYAxis = "true";
                    shortRow = "false";
                }
                output.append(" row: { show: ");
                output.append(showYAxis);
                output.append(",shortRow:");
                output.append(shortRow);
                output.append(", showYAxis:");
                output.append(showYAxis);
                output.append("}, data:[");
                TreeMap<String, Double> data = dataMap.get(seriesName);
                for (String dp : data.keySet()) {
                    int rangeLabel = 0;
                    if (counter2 != 0) {
                        output.append(",");
                    }
                    if (xLabel.equals("Time")) {
                        if (data.get(dp) == Double.NaN) {
                            output.append("[\"");
                            output.append(dp);
                            output.append("\",NULL]");
                        } else {
                            output.append("[\"");
                            output.append(dp);
                            output.append("\",");
                            output.append(data.get(dp));
                            output.append("]");
                        }
                    } else {
                        long value = xLabelRangeHash.get(dp);
                        if (data.get(dp) == Double.NaN) {
                            output.append("[\"");
                            output.append(value);
                            output.append("\",NULL]");
                        } else {
                            output.append("[\"");
                            output.append(value);
                            output.append("\",");
                            output.append(data.get(dp));
                            output.append("]");
                        }
                        rangeLabel++;
                    }
                    counter2++;
                }
                output.append("], min:0");
                if (this.userDefinedMax) {
                    output.append(", max:");
                    output.append(this.max);
                }
                output.append("}");
                counter++;
            }
            i++;
        }          
        output.append(" ];\n");
        if(request!=null && request.getParameter("format")==null) {
            output.append(" wholePeriod();</script></body></html>\n");
        } else {
            output.append("chartTitle=\"<center>"+this.title+"</center>\";");
            output.append("height="+this.height+";");
        }
    	return output.toString();
    }            
}
