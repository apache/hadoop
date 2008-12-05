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
<%@ taglib prefix="my" uri="/WEB-INF/jsp2/taglib.tld" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<table class="portal_table" cellpadding="2" cellspacing="0" width="100%">
<thead><tr bgcolor="lightgrey">
<th>Name</th>
<th>Owner</th>
<th>Permission</th>
<th colspan="4">Operations</th>
</tr></thead>
<%
  String[] users= {"Mac", "Eric", "Linyun", "Rounpin", "Jerome", "admin"};
  pageContext.setAttribute("users", users);
  pageContext.setAttribute("user","admin");
  pageContext.setAttribute("permission.all.read","1");
  pageContext.setAttribute("permission.all.write","2");
  pageContext.setAttribute("default_view","default");
%>
<my:findViews>
	<tr><td>
<div id="displayViewNameBlock${key}">
<a href="#" onClick="javascript:set_current_view('${key}');initScript('${key}');">${description}</a></div>
<div id="changeViewNameBlock${key}" style="display:none;">
<input type=text class=formInput name="changeViewNameField${key}" id="changeViewNameField${key}" value='${description}'/>
</div>
</td><td align='center'>${owner}</td>
<td align='center' width="270">
<!-- begin permission -->
<div id="displayViewPermissionBlock${key}" style="display:block;">
<c:choose>
  <c:when test="${permission.all.read=='1'}">
    Public
  </c:when>
  <c:otherwise>
    Private
  </c:otherwise>
</c:choose>
</div>
<div id="editViewPermissionBlock${key}" style="display:none;">
  <div id="permission${key}">
<input type=hidden name="permissionRowCount${key}" value="${fn:length(permissions)}"/>
<table class="portal_table" id="permissionTable${key}" cellspacing=0 cellpadding=0>
<thead><tr bgcolor="lightgrey"><th>User</th><th>Allow To Read</th><th>Allow to Modify</th><th></th></tr></thead>
<tbody>
<!-- begin permission -->
<c:forEach items="${permission}" var="pK">
  <tr><td>
    <select id='userid_permission' class='formSelect'>
    <c:forEach items="${users}" var="user">
      <c:choose>
        <c:when test="${user==pK}">
          <option value='${user}' selected>${user}</option>
        </c:when>
        <c:otherwise>
          <option value='${user}'>${user}</option>
        </c:otherwise>
      </c:choose>
    </c:forEach>
    </select>
  </td><td align="middle">
    <input type='checkbox' class='formCheckbox' id='read_permission' name='read_permission' ${permission.[permissionKey].read ? "checked":""}>
  </td><td align="middle">
    <input type='checkbox' class='formCheckbox' id='modify_permission' name='modify_permission' ${permission.[permissionKey].modify ? "checked":""}>
  </td><td>
    <img src='/sim/images/close16.gif' onClick="deleteCurrentRow('${key}',this);"/>
  </td></tr>
</c:forEach>
<!-- end permission -->
</tbody>
</table>
<br/>
<input type=button class=formButton name="addPermissionRowButton" value="Add More Permission" onClick="addPermissionRow('${key}','',false,false);"/>
  </div>
</div>
<!-- end permission -->
</td><td width="115">
<!-- begin operations -->
<!--c:if test="${permission.all.modify=='1'} || ${permission.user.modify=='1'}"-->
  <div id="changeViewNameButtonBlock${key}" style="display:block;">
    <input class="formButton" name="changeNameButton${key}" id="changeNameButton${key}" value="Change" type="button" onClick="changeViewName('${key}',false);"/>&nbsp;
  </div>
  <div id="cancelChangeViewNameBlock${key}" style="display:none;">
    <input class="formButton" name="saveNameButton${key}" id="saveNameButton${key}" value="Save" type="button" onClick="changeViewName('${key}',true);"/>&nbsp;
    <input class="formButton" name="cancelChangeNameButton${key}" id="cancelChangeNameButton${key}" value="Cancel" type="button" onClick="changeViewName('${key}',false);"/>&nbsp;
  </div>
<!--/c:if-->
</td>
<td nowrap width="60">
<input class="formButton" name="cloneview" value="Clone" type="button" onClick="cloneView('${key}');"/>&nbsp;
</td>
<td nowrap width="60">
<!--c:if test="${key=='default'}"-->
<!--  <input class="formButton" name="restoreview" value="Restore" type="button" onClick="if (confirm(Are you sure you want to restore the default workspace?)){ restoreView('${key}');}"/> -->
<!--/c:if-->

<!--c:if test="${permision.all.modify=='1'} || ${permission.user.modify=='1'} && ${key!='default'}"-->
  <input class="formButton" name="delelete" value="Delete" type="button" onClick="if (confirm('Are you sure you want to delete the workspace?')){ deleteView('${key}');}"/>
<!--/c:if-->
</td>
<td nowrap width="60">
<c:choose>
  <c:when test="${key}!=${default_view}"> 
    <input class="formButton" name="setDefault" value="Set As Default" type="button" onClick="setDefaultView('${key}');"/>
  </c:when>
  <c:otherwise>
    Default View
  </c:otherwise>
</c:choose>
</td>
<!-- end operation -->
</td></tr>
</my:findViews>
</table>
