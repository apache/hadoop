#! /usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This file is used to fix the paths in CNDOCS_SRC/uming.conf and
# CNDOCS_SRC/src/documentation/sitemap.xmap 

import os
import sys

template_uming = \
'''<?xml version="1.0"?>
<configuration>
  <fonts>
    <font metrics-file="%s/uming.xml" kerning="yes" embed-file="%s/uming.ttc">
      <font-triplet name="AR PL UMing" style="normal" weight="normal"/>
      <font-triplet name="AR PL UMing" style="italic" weight="normal"/>
      <font-triplet name="AR PL UMing" style="normal" weight="bold"/>
      <font-triplet name="AR PL UMing" style="italic" weight="bold"/>
    </font>
  </fonts>
</configuration>
'''

template_sitemap = \
'''<?xml version="1.0"?>
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
<map:sitemap xmlns:map="http://apache.org/cocoon/sitemap/1.0">
  <map:components>
    <map:serializers default="fo2pdf">
      <map:serializer name="fo2pdf"
                src="org.apache.cocoon.serialization.FOPSerializer"
                mime-type="application/pdf">
        <user-config src="%s/uming.conf"/>
        </map:serializer>
    </map:serializers>
  </map:components>
  <map:pipelines>
    <map:pipeline>
<!-- generate .pdf files from .fo -->
      <map:match type="regexp" pattern="^(.*?)([^/]*).pdf$">
        <map:select type="exists">
          <map:when test="{lm:project.{1}{2}.pdf}">
            <map:read src="{lm:project.{1}{2}.pdf}"/>
          </map:when>
          <map:when test="{lm:project.{1}{2}.fo}">
            <map:generate src="{lm:project.{1}{2}.fo}"/>
            <map:serialize type="fo2pdf"/>
          </map:when>
          <map:otherwise>
            <map:generate src="cocoon://{1}{2}.fo"/>
            <map:serialize type="fo2pdf"/>
          </map:otherwise>
        </map:select>
      </map:match>
    </map:pipeline>
  </map:pipelines>
</map:sitemap>
'''

def main(argv=None):
  if argv is None:
    argv = sys.argv[1:3]
  cndocs_src = argv[0]

  uming_file = 'src/docs/cn/uming.conf'
  fout = open(uming_file, "w")
  fout.write(template_uming % (cndocs_src, cndocs_src))
  fout.close()
  
  site_map_file = 'src/docs/cn/src/documentation/sitemap.xmap'
  fout = open(site_map_file, "w")
  fout.write(template_sitemap % (cndocs_src))
  fout.close()
  return 0

##########################
if __name__ == "__main__":
  sys.exit(main())
