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
<%@ page import = "javax.servlet.http.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler"  %>
<%
    if(session.getAttribute("cluster")==null) {
        ClusterConfig cc = new ClusterConfig();
        Iterator ci = cc.getClusters();
        String cluster = (String) ci.next();
        session.setAttribute("cluster", cluster);
    }
    if(session.getAttribute("period")==null || session.getAttribute("start")==null || session.getAttribute("end")==null ||
       session.getAttribute("time_type")==null) {
        session.setAttribute("time_type","last");
        session.setAttribute("period","last1hr");
        long now = Calendar.getInstance().getTime().getTime();
        session.setAttribute("start",now-(60*60*1000));
        session.setAttribute("end",now);
        TimeHandler time = new TimeHandler(request);
    }
    String machine="";
    if(session.getAttribute("hosts")==null) {
        session.setAttribute("hosts",machine);
    }
    if(((String)request.getHeader("user-agent")).indexOf("iPhone")>0) { %>
        <jsp:forward page="iphone.jsp" />
<%  }

%>
<html><title>Hadoop Infrastructure Care Center</title>
<body id="main_body">
<div id="debug"></div>
<div id="shadow" class="shadow"> 
<table width="100%" height="100%"><tr><td valign="center" align="middle">
<table padding="20px">
<tr><td>
<font size="32" color="#eeeeee">&nbsp;Loading...&nbsp;</font>&nbsp;</td></tr>
</table>
</td></tr></table>
</div>
<link href="css/default.css" rel="stylesheet" type="text/css">
<style type="text/css">@import url('css/calendar.css');</style>
<link href="css/menu.css" rel="stylesheet" type="text/css">
<link href="css/tab.css" rel="stylesheet" type="text/css">
<link href="css/timeframe.css" rel="stylesheet" type="text/css">
<link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/2.6.0/build/fonts/fonts-min.css" />
<link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/2.6.0/build/treeview/assets/skins/sam/treeview.css" />
<!-- all the necessary js files -->
<script type="text/javascript" src="./js/yahoo-dom-event.js"></script>
<script type="text/javascript" src="./js/treeview-min.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/prototype.js"></script>
<script type="text/javascript" src="/hicc/js/calendar.js"></script>
<script type="text/javascript" src="/hicc/js/timeframe.js"></script>
<script type="text/javascript" src="/hicc/js/lang/calendar-en.js"></script>
<script type="text/javascript" src="/hicc/js/calendar-setup.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/scriptaculous.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/effects.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/dragdrop.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/workspace.js"></script>
<script type="text/javascript" src="/hicc/js/slider.js"></script>
<script type="text/javascript" src="/hicc/js/swfobject.js"></script>

<script type="text/javascript">
var _users_list=''; //'[% users_list_json %]'.evalJSON();
var expanded_page=1;
var need_save=0;
window.onbeforeunload = check_save;
</script>
<script type="text/javascript">
function toggle_view_all() {
    if(expanded_page) {
        document.getElementById('view_all').src='/hicc/images/stop.png';
        _currentView.getCurrentPage().collapse_all();
        expanded_page=0;
    } else {
        document.getElementById('view_all').src='/hicc/images/stop.png';
        _currentView.getCurrentPage().expand_all();
        expanded_page=1;
    }
}
</script>
<input type=hidden name=cmd id=cmd value=1>
<table width="100%" cellpadding=3 cellspacing=0>
<tr><td nowrap><img src="images/chukwa.jpg" align="absmiddle"> Hadoop Infrastructure Care Center</td>
    <td align="left" nowrap class="portal_top_nav_bar" nowrap> </td></tr>
</table>
<table width="100%" cellpadding=0 cellspacing=0 class="menubar">
<tr><td>
<div id="menu">
<ul>
  <li><select class="formSelect" id="currentpage" onChange="changeView(this);"></select></li>
</ul>
<ul>
  <li><a href="#" onClick="return false;"><img src="/hicc/images/application.png" border="0" align="absmiddle"> Options</a>
    <ul>
      <table>
        <tr><td onClick="javascript:add_widget_menu();"><img src="/hicc/images/add_widget.png"> Add Widget</td></tr>
        <tr><td onClick="javascript:addColumn();"><img src="/hicc/images/layout_add.png"> Add Column</td></tr>
        <tr><td onClick="javascript:deleteColumn();"><img src="/hicc/images/layout_delete.png"> Delete Last Column</td></tr>
        <tr><td onClick="javascript:addNewPage();"><img src="/hicc/images/tab_add.png"> Add New Tab</td></tr>
        <tr><td onClick="javascript:deleteCurrentPage();"><img src="/hicc/images/tab_delete.png"> Delete Current Tab</td></tr>
      </table>
    </ul>
  </li>
</ul>
<ul>
  <li><a href="#" onClick="javascript:manage_content(); return false;"><img src="/hicc/images/table.png" border="0" align="absmiddle"> Workspace Builder</a></li>
</ul>
<ul>
  <li><a href="#" onClick="saveView(); return false;"><img src="/hicc/images/drive.png" border="0" align="absmiddle"> Save Dashboard</a></li>
</ul>
</div>
</td><td>
</td><td align="right"><a href='#' onclick='toggle_view_all()' class='glossy_icon'><img id='view_all' src='/hicc/images/stop.png' border=0></a>  <a href='#' onclick='_currentView.getCurrentPage().refresh_all();' class='glossy_icon'><img src='/hicc/images/refresh.png' border=0></a>&nbsp;</div>
</td></tr>
</table>
<table width="100%" cellpadding=0 cellspacing=0>
<tr><td colspan="3">
<!-- first page for manage view -->
<div id="manage_view" style="display:none;overflow:hidden;width:100%;">
<table cellspacing="10" cellpadding="0" width="100%" class="ppsmenu">
<tr><td>
<table width="100%" class="titlebar"><tr><td>Workspace</td><td align="right"><span class="glossy_icon"><a href="#" onClick="manage_content('close');"><img src="images/close.png" align="absmiddle"></a></span></td></tr></table>
<div id="views_list_div">
</div>
</td></tr>
<tr><td colspan="2">
<input class="formButton" type="button" name="new_workspace" value="Create New Workspace" onClick="createNewView();"/>
</td></tr>
</table>
</div>
<!-- end of manage view -->

<!-- panel 3 for creation widgets -->
<div id="widget_view" style="display:none;overflow:hidden;width:100%;">
<table cellspacing="10" cellpadding="0" width="100%" class="ppsmenu">
<tr><td>
<table width=100% class="titlebar"><tr><td>Widget Menu</td><td width="18"><span class="glossy_icon"><a href="#" onClick="add_widget_menu('close');"><img src="images/close.png"></span></a></td></tr></table>
<table width="100%" cellpadding="1" cellspacing="1" class="mailListBorder">
<tr><td class="table-subhead" align="center">Widgets Catalog</td><td class="table-subhead" align="center">Widget Details</td></tr>
<tr><td valign="top" width="250" class="white">
<table class="menu_table"><tr><td>
<div id="myWidgetContainer" style="width: 250px; height: 300px; border: none;overflow: auto;>
     <span id="treePlaceHolder"
        style="background-color:#F00; color:#FFF;">
       Loading tree widget...
     </span>
</div>
</td></tr></table>
</td><td valign="top" class="configurationTableContent">
<table width="100%" cellpadding="5" cellspacing="0">
<tr><td>
<div id='widget_detail' class="configurationTableContent">
Select the widget from the widget tree to see the detail.
</div>
</td></tr>
</table>

</td></tr>
</table>
</td></tr>
</table>
</div>
<DIV ID="popup-div" class="ppsPopup" STYLE="max-width:300px;position:absolute;visibility:hidden;z-index:100;">
</DIV>
<!-- end of panel 3 -->
<!-- page selector -->
<table class="page_selector_table" width="100%" cellspacing=0 cellpadding=0>
<tr><td>
<div id="page_selector" style="display:block;">
<ul id="tablist">
</ul>
</div>
</td>
</tr>
</table>
</td></tr>
<tr><td bgcolor="white" id="workspaceContainer" colspan="3">
<!-- content location -->
</td></tr>
</table>

<div id="rectangleDiv" style="display:none;">
</div>

<!-- page configuration UI -->
<div id="page_config_menu" style="display:none;">
<table width="160" cellpadding=0 cellspacing=0><tr><td width="80" class="ppsPopupHead">&nbsp;</td><td class="ppsPopupTail"><img src="/hicc/css/images/popup_tail.gif"></td><td class="ppsPopupHead">&nbsp;</td><td width="5"></td></tr></table>
<table width="160" style="border-style:solid; border-top:0; border-left:0; border-width:5px; border-color:#CCCCCC; " cellpadding=0 cellspacing=0>
<tr><td>
<table class="ppPopupBody">
<tr><td class="xMessageHeaderLabel">Page Configuration Menu</td><td align="right">
</td></tr>
<tr><td class="ppsmenu"><a href="#" class="ppsmenu" onclick="addColumn();configPage('');">Add a column</a>
</td></tr>
<tr><td class="ppsmenu"><a href="#" class="ppsmenu" onclick="deleteColumn();configPage('');">Delete last column</a>
</td></tr>
</table>
</td></tr>
</table>
</div>
<!-- end of page configuration UI -->

<script>
// initialize the script
update_views_list();
initScript('<% if(request.getParameter("view") != null) { out.print(request.getParameter("view")); } else { out.print("default"); } %>');
set_current_view('<% if(request.getParameter("view") != null) { out.print(request.getParameter("view")); } else { out.print("default"); } %>');
$('shadow').style.display='none';

function debugMode() {
}

</script>
</body>
</html>
