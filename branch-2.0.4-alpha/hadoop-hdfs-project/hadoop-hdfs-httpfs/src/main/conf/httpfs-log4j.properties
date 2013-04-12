#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
#

# If the Java System property 'httpfs.log.dir' is not defined at HttpFSServer start up time
# Setup sets its value to '${httpfs.home}/logs'

log4j.appender.httpfs=org.apache.log4j.DailyRollingFileAppender
log4j.appender.httpfs.DatePattern='.'yyyy-MM-dd
log4j.appender.httpfs.File=${httpfs.log.dir}/httpfs.log
log4j.appender.httpfs.Append=true
log4j.appender.httpfs.layout=org.apache.log4j.PatternLayout
log4j.appender.httpfs.layout.ConversionPattern=%d{ISO8601} %5p %c{1} [%X{hostname}][%X{user}:%X{doAs}] %X{op} %m%n

log4j.appender.httpfsaudit=org.apache.log4j.DailyRollingFileAppender
log4j.appender.httpfsaudit.DatePattern='.'yyyy-MM-dd
log4j.appender.httpfsaudit.File=${httpfs.log.dir}/httpfs-audit.log
log4j.appender.httpfsaudit.Append=true
log4j.appender.httpfsaudit.layout=org.apache.log4j.PatternLayout
log4j.appender.httpfsaudit.layout.ConversionPattern=%d{ISO8601} %5p [%X{hostname}][%X{user}:%X{doAs}] %X{op} %m%n

log4j.logger.httpfsaudit=INFO, httpfsaudit

log4j.logger.org.apache.hadoop.fs.http.server=INFO, httpfs
log4j.logger.org.apache.hadoop.lib=INFO, httpfs
