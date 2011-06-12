<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<svg width="20pt" height="20pt"
   xmlns="http://www.w3.org/2000/svg"
   xmlns:xlink="http://www.w3.org/1999/xlink">
  <defs
     id="defs550">
    <linearGradient id="gray2white">
      <stop style="stop-color:#7f7f7f;stop-opacity:1;" offset="0.000000"/>
      <stop style="stop-color:#ffffff;stop-opacity:1;" offset="1.000000"/>
    </linearGradient>
    <linearGradient id="pageshade" xlink:href="#gray2white"
       x1="0.95" y1="0.95"
       x2="0.40" y2="0.20"
       gradientUnits="objectBoundingBox" spreadMethod="pad" />
    <path d="M 0 0 L 200 0" style="stroke:#000000;stroke-width:1pt;" id="hr"/>
  </defs>
  <g transform="scale(0.08)">
    <g transform="translate(40, 0)">
      <rect width="230" height="300" x="0" y="0"
            style="fill:url(#pageshade);fill-rule:evenodd;
            stroke:#000000;stroke-width:1.25;"/>
      <g transform="translate(15, 60)">
        <use xlink:href="#hr" x="0" y="0"/>
        <use xlink:href="#hr" x="0" y="60"/>
        <use xlink:href="#hr" x="0" y="120"/>
        <use xlink:href="#hr" x="0" y="180"/>
      </g>
    </g>
    <g transform="translate(0,70),scale(1.1,1.6)">
      <rect width="200" height="100" x="0" y="0"
         style="fill:#ff0000;fill-rule:evenodd;
                stroke:#000000;stroke-width:2.33903;"/>
      <text x="20" y="75"
            style="stroke:#ffffff;stroke-width:1.0;
                   font-size:72;font-weight:normal;fill:#ffffff;
                   font-family:Arial;text-anchor:start;">POD</text>
    </g>
  </g>
</svg>
