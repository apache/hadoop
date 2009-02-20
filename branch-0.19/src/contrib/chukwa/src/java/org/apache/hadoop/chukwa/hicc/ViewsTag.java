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

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.chukwa.hicc.Views;

public class ViewsTag extends SimpleTagSupport {
    private String key = null;
    private String owner = null;
    private String description = null;
    Views views = new Views();
    public void doTag() throws JspException, IOException {
        for(int i=0;i<views.length();i++) {
                int j=0;
                getJspContext().setAttribute( "key", views.getKey(i) );
                Iterator permission = views.getPermission(i);
                String[] authUsers = new String[100];
                for ( Iterator perm = permission; perm.hasNext(); ) {
                    String who = perm.next().toString();
                    authUsers[j]=who;
//                    getJspContext().setAttribute( "permission."+who+".read", views.getReadPermission(i,who) );
//                    getJspContext().setAttribute( "permission."+who+".write", views.getWritePermission(i,who) );
                    j=j+1;
                }
//                getJspContext().setAttribute( "permission", authUsers );
                getJspContext().setAttribute( "owner", views.getOwner(i) );
                getJspContext().setAttribute( "description", views.getDescription(i) );
	        getJspBody().invoke(null);
        }
    }

}

