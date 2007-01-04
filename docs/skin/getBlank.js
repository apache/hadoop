/*
* Copyright 2002-2004 The Apache Software Foundation or its licensors,
* as applicable.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/**
 * This script, when included in a html file and called from a form text field, will set the value of this field to ""
 * if the text value is still the standard value.
 *
 * Typical usage:
 * <script type="text/javascript" language="JavaScript" src="getBlank.js"></script>
 * <input type="text" id="query" value="Search the site:" onFocus="getBlank (this, 'Search the site:');"/>
 */
<!--
function getBlank (form, stdValue){
if (form.value == stdValue){
	form.value = '';
	}
return true;
}
//-->
