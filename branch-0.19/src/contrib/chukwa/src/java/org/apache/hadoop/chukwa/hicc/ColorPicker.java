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

public class ColorPicker {
	private String color = "#ff5757";

	public ColorPicker() {
    	color = "#ff5757";
    }
	
    public String get(int counter) {
        if((counter % 6)==0) {
            String cyan = Integer.toHexString(256-(counter % 255));
            color = "#57"+cyan+cyan;
        } else if((counter % 5)==0) {
            String purple = Integer.toHexString(256-(counter % 255));
            color = "#"+purple+"57"+purple;
        } else if((counter % 4)==0) {
            String yellow = Integer.toHexString(256-(counter % 255 * 20));
            color = "#FF"+yellow+"00";
        } else if((counter % 3)==0) {
            String green = Integer.toHexString(256-(counter % 255));
            color = "#57"+green+"57";
        } else if((counter % 2)==0) {
            String blue = Integer.toHexString(256-(counter % 255));
            color = "#5757"+blue+"";
        } else {
            color = "#ff5757";
        }
        return this.color;
    }
}
