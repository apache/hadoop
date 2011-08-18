<%
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
%>
<%!private static final long serialVersionUID = 1L;%>
<%@ page
contentType="text/html; charset=UTF-8"
import="org.apache.hadoop.mapred.*"
import="javax.servlet.*"
import="javax.servlet.http.*"
import="java.io.*"
import="java.util.*"
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Job Queue Information page</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  QueueManager qmgr = tracker.getQueueManager();
  JobQueueInfo[] rootQueues = qmgr.getRootQueues();
%>
<%!
  public static String getTree(String parent, JobQueueInfo[] rootQueues) {
    List<JobQueueInfo> rootQueueList = new ArrayList<JobQueueInfo>();
    for (JobQueueInfo queue : rootQueues) {
      rootQueueList.add(queue);
    }
    return getTree(parent, rootQueueList);
  }

  private static String getTree(String parent, List<JobQueueInfo> children) {
    StringBuilder str = new StringBuilder();
    if (children == null) {
      return "";
    }
    for (JobQueueInfo queueInfo : children) {
      String variableName = queueInfo.getQueueName().replace(":", "_");
      String label = queueInfo.getQueueName().split(":")[queueInfo
          .getQueueName().split(":").length - 1];
      str.append(String.format(
          "var %sTreeNode = new YAHOO.widget.MenuNode(\"%s\", %s, false);\n",
          variableName, label, parent));
      str.append(String.format("%sTreeNode.data=\"%s\";\n", variableName,
          queueInfo.getSchedulingInfo().replaceAll("\n", "<br/>")));
      str.append(String.format("%sTreeNode.name=\"%s\";\n", variableName,
          queueInfo.getQueueName()));
      str.append(getTree(variableName + "TreeNode", queueInfo.getChildren()));
    }
    return str.toString();
  }
%>
<style type="text/css">
  /*margin and padding on body element
    can introduce errors in determining
    element position and are not recommended;
    we turn them off as a foundation for YUI
    CSS treatments. */
  body {
    margin:0;
    padding:0;
  }
</style>
<!-- Combo-handled YUI CSS files: --> 
<link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/combo?2.7.0/build/fonts/fonts-min.css&2.7.0/build/grids/grids-min.css&2.7.0/build/base/base-min.css&2.7.0/build/assets/skins/sam/skin.css"> 
<!-- Combo-handled YUI JS files: --> 
<script type="text/javascript" src="http://yui.yahooapis.com/combo?2.7.0/build/utilities/utilities.js&2.7.0/build/layout/layout-min.js&2.7.0/build/container/container_core-min.js&2.7.0/build/menu/menu-min.js&2.7.0/build/stylesheet/stylesheet-min.js&2.7.0/build/treeview/treeview-min.js"></script>
</head>
<body class="yui-skin-sam">
<div id="left">
<div id="queue_tree"></div>
</div>
<div id="right">
  <div id="right_top" width="100%"></div>
  <div style="text-align: center;"><h2><a href="jobtracker.jsp">Job Tracker</a>
  </h2></div>
</div>
<script type = "text/javascript">
if (typeof(YAHOO) == "undefined") {
  window.location = "queuetable.jsp";
}
else {
  (function() {
    var tree;
    YAHOO.util.Event.onDOMReady(function() {
      var layout = new YAHOO.widget.Layout({
        units : [
          { position: 'center', body: 'right', scroll: true},
          { position: 'left', width: 150, gutter: '5px', resize: true, 
            body:'left',scroll: true, collapse:true,
            header: '<center>Queues</center>'
          }
        ]
      });
      layout.on('render', function() {
        function onLabelClick(node) {
          var schedulingInfoDiv = document.getElementById('right_top');
          schedulingInfoDiv.innerHTML = 
            "<font size=\"+3\"><b><u>Scheduling Information for queue: " +
             node.label + "</u></b></font><br/><br/>" + node.data + "<hr/>";
          var surl = 'jobtable.jsp?queue_name='+node.name;
          var callback = 
            {success: handleSuccess, failure: handleFailure, arguments: {}};
          var request = YAHOO.util.Connect.asyncRequest('GET', surl, callback); 
        }       
        function handleSuccess(o) {
    	  var jobtablediv = document.getElementById('right_top');
          jobtablediv.innerHTML += o.responseText;
        }
        function handleFailure(o) {
    	  var jobtablediv = document.getElementById('right_top');
    	  jobtablediv.innerHTML = 'unable to retrieve jobs for the queue'; 
        }
        tree = new YAHOO.widget.TreeView("queue_tree");
        <%=getTree("tree.getRoot()", rootQueues)%>
        tree.subscribe("labelClick", onLabelClick);
        tree.draw();
        onLabelClick(tree.getRoot().children[0]);
      });
      layout.render();
    });
  })();
}
</script>
</body>
</html>
