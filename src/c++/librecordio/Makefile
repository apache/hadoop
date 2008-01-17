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

COPTS=-g3 -O0 -Wall

all: librecordio.a test

librecordio.a: recordio.o filestream.o binarchive.o csvarchive.o xmlarchive.o \
	exception.o typeIDs.o fieldTypeInfo.o recordTypeInfo.o utils.o
	ar cru librecordio.a recordio.o filestream.o binarchive.o csvarchive.o xmlarchive.o \
	exception.o typeIDs.o fieldTypeInfo.o recordTypeInfo.o utils.o

recordio.o: recordio.cc recordio.hh archive.hh
	g++ ${COPTS} -c -I${XERCESCROOT}/include -o recordio.o recordio.cc

filestream.o: filestream.cc recordio.hh filestream.hh
	g++ ${COPTS} -c -o filestream.o filestream.cc

binarchive.o: binarchive.cc recordio.hh binarchive.hh archive.hh
	g++ ${COPTS} -c -o binarchive.o binarchive.cc

csvarchive.o: csvarchive.cc recordio.hh csvarchive.hh archive.hh
	g++ ${COPTS} -c -o csvarchive.o csvarchive.cc

xmlarchive.o: xmlarchive.cc recordio.hh xmlarchive.hh archive.hh
	g++ ${COPTS} -c -I${XERCESCROOT}/include -o xmlarchive.o xmlarchive.cc

exception.o: exception.cc exception.hh
	g++ ${COPTS} -c -o exception.o exception.cc

typeIDs.o: typeIDs.cc typeIDs.hh
	g++ ${COPTS} -c -o typeIDs.o typeIDs.cc
fieldTypeInfo.o: fieldTypeInfo.cc fieldTypeInfo.hh
	g++ ${COPTS} -c -o fieldTypeInfo.o fieldTypeInfo.cc
recordTypeInfo.o: recordTypeInfo.cc recordTypeInfo.hh
	g++ ${COPTS} -c -o recordTypeInfo.o recordTypeInfo.cc
utils.o: utils.cc utils.hh
	g++ ${COPTS} -c -o utils.o utils.cc

recordio.cc: recordio.hh archive.hh exception.hh
filestream.cc: recordio.hh filestream.hh 
binarchive.cc: recordio.hh binarchive.hh 
csvarchive.cc: recordio.hh csvarchive.hh 
xmlarchive.cc: recordio.hh xmlarchive.hh 
exception.cc: exception.hh 
typeIDs.cc: typeIDs.hh
fieldTypeInfo.cc: fieldTypeInfo.hh
recordTypeInfo.cc: recordTypeInfo.hh
utils.cc: utils.hh

test: librecordio.a
	make -C test all

clean:
	rm -f *~ *.o *.a
	make -C test clean
