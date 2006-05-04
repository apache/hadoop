#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

all: test testFromJava

test: test.o test.jr.o
	g++ -g3 -O0 -o test test.o test.jr.o -L.. -L${XERCESCROOT}/lib -lrecordio -lxerces-c
	
test.o: test.cc
	g++ -g3 -O0 -c -I.. -o test.o test.cc

testFromJava: testFromJava.o test.jr.o
	g++ -g3 -O0 -o testFromJava testFromJava.o test.jr.o -L.. -L${XERCESCROOT}/lib -lrecordio -lxerces-c
	
testFromJava.o: testFromJava.cc
	g++ -g3 -O0 -c -I.. -o testFromJava.o testFromJava.cc


test.jr.o: test.jr.cc
	g++ -g3 -O0 -c -I.. -o test.jr.o test.jr.cc

%.jr.cc %.jr.hh: %.jr
	${HADOOP_HOME}/bin/rcc --language c++ $<

%: %.o
%: %.cc

test.cc: test.hh
test.hh: test.jr.hh ../recordio.hh ../filestream.hh

clean:
	rm -f *~ *.o test testFromJava *.jr.*

