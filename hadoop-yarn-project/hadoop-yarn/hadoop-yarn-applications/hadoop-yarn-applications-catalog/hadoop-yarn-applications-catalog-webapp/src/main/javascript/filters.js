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
var app = angular.module("filters", []);

app.filter("counterValue",function(){
  return function(value){
    var count=parseInt(value), suffix="";
    if (count>=1000000){
      count=Math.round(count/1000000);
      suffix="M"
    } else if (count>=1000){
      count=Math.round(count/1000);
      suffix="K"
    }
    return""+count+suffix
  }
});
