#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at#
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

BUILDROOT=../../../../build/contrib/sqoop
BUILD_DIR=$(BUILDROOT)/doc

all: man userguide

man: $(BUILD_DIR)/sqoop.1.gz

userguide: $(BUILD_DIR)/SqoopUserGuide.html

$(BUILD_DIR)/sqoop.1.gz: Sqoop-manpage.txt *formatting*.txt
	asciidoc -b docbook -d manpage Sqoop-manpage.txt
	xmlto man Sqoop-manpage.xml
	gzip sqoop.1
	rm Sqoop-manpage.xml
	mkdir -p $(BUILD_DIR)
	mv sqoop.1.gz $(BUILD_DIR)

$(BUILD_DIR)/SqoopUserGuide.html: SqoopUserGuide.txt *.txt
	asciidoc SqoopUserGuide.txt
	mkdir -p $(BUILD_DIR)
	mv SqoopUserGuide.html $(BUILD_DIR)

clean:
	-rm $(BUILD_DIR)/sqoop.1.gz
	-rm $(BUILD_DIR)/SqoopUserGuide.html

.PHONY: all man userguide clean

