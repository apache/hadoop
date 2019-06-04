---
title: "Audit Parser"
date: 2018-12-17
menu:
   main:
      parent: Tools
---
<!---
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

Audit Parser tool can be used for querying the ozone audit logs.
This tool creates a sqllite database at the specified path. If the database
already exists, it will avoid creating a database.

The database contains only one table called `audit` defined as:

CREATE TABLE IF NOT EXISTS audit (
datetime text,
level varchar(7),
logger varchar(7),
user text,
ip text,
op text,
params text,
result varchar(7),
exception text,
UNIQUE(datetime,level,logger,user,ip,op,params,result))

Usage:
{{< highlight bash >}}
ozone auditparser <path to db file> [COMMAND] [PARAM]
{{< /highlight >}}

To load an audit log to database:
{{< highlight bash >}}
ozone auditparser <path to db file> load <path to audit log>
{{< /highlight >}}
Load command creates the audit table described above.

To run a custom read-only query:
{{< highlight bash >}}
ozone auditparser <path to db file> query <select query enclosed within double quotes>
{{< /highlight >}}

Audit Parser comes with a set of templates(most commonly used queries).

To run a template query:
{{< highlight bash >}}
ozone auditparser <path to db file> template <templateName>
{{< /highlight >}}

Following templates are available:

|Template Name|Description|SQL|
|----------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
|top5users|Top 5 users|select user,count(*) as total from audit group by user order by total DESC limit 5|
|top5cmds|Top 5 commands|select op,count(*) as total from audit group by op order by total DESC limit 5|
|top5activetimebyseconds|Top 5 active times, grouped by seconds|select substr(datetime,1,charindex(',',datetime)-1) as dt,count(*) as thecount from audit group by dt order by thecount DESC limit 5|
