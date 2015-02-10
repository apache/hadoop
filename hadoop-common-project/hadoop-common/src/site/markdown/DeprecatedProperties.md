<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Deprecated Properties
=====================

The following table lists the configuration property names that are deprecated in this version of Hadoop, and their replacements.

| **Deprecated property name** | **New property name** |
|:---- |:---- |
| create.empty.dir.if.nonexist | mapreduce.jobcontrol.createdir.ifnotexist |
| dfs.access.time.precision | dfs.namenode.accesstime.precision |
| dfs.backup.address | dfs.namenode.backup.address |
| dfs.backup.http.address | dfs.namenode.backup.http-address |
| dfs.balance.bandwidthPerSec | dfs.datanode.balance.bandwidthPerSec |
| dfs.block.size | dfs.blocksize |
| dfs.data.dir | dfs.datanode.data.dir |
| dfs.datanode.max.xcievers | dfs.datanode.max.transfer.threads |
| dfs.df.interval | fs.df.interval |
| dfs.federation.nameservice.id | dfs.nameservice.id |
| dfs.federation.nameservices | dfs.nameservices |
| dfs.http.address | dfs.namenode.http-address |
| dfs.https.address | dfs.namenode.https-address |
| dfs.https.client.keystore.resource | dfs.client.https.keystore.resource |
| dfs.https.need.client.auth | dfs.client.https.need-auth |
| dfs.max.objects | dfs.namenode.max.objects |
| dfs.max-repl-streams | dfs.namenode.replication.max-streams |
| dfs.name.dir | dfs.namenode.name.dir |
| dfs.name.dir.restore | dfs.namenode.name.dir.restore |
| dfs.name.edits.dir | dfs.namenode.edits.dir |
| dfs.permissions | dfs.permissions.enabled |
| dfs.permissions.supergroup | dfs.permissions.superusergroup |
| dfs.read.prefetch.size | dfs.client.read.prefetch.size |
| dfs.replication.considerLoad | dfs.namenode.replication.considerLoad |
| dfs.replication.interval | dfs.namenode.replication.interval |
| dfs.replication.min | dfs.namenode.replication.min |
| dfs.replication.pending.timeout.sec | dfs.namenode.replication.pending.timeout-sec |
| dfs.safemode.extension | dfs.namenode.safemode.extension |
| dfs.safemode.threshold.pct | dfs.namenode.safemode.threshold-pct |
| dfs.secondary.http.address | dfs.namenode.secondary.http-address |
| dfs.socket.timeout | dfs.client.socket-timeout |
| dfs.umaskmode | fs.permissions.umask-mode |
| dfs.write.packet.size | dfs.client-write-packet-size |
| fs.checkpoint.dir | dfs.namenode.checkpoint.dir |
| fs.checkpoint.edits.dir | dfs.namenode.checkpoint.edits.dir |
| fs.checkpoint.period | dfs.namenode.checkpoint.period |
| fs.default.name | fs.defaultFS |
| hadoop.configured.node.mapping | net.topology.configured.node.mapping |
| hadoop.job.history.location | mapreduce.jobtracker.jobhistory.location |
| hadoop.native.lib | io.native.lib.available |
| hadoop.net.static.resolutions | mapreduce.tasktracker.net.static.resolutions |
| hadoop.pipes.command-file.keep | mapreduce.pipes.commandfile.preserve |
| hadoop.pipes.executable.interpretor | mapreduce.pipes.executable.interpretor |
| hadoop.pipes.executable | mapreduce.pipes.executable |
| hadoop.pipes.java.mapper | mapreduce.pipes.isjavamapper |
| hadoop.pipes.java.recordreader | mapreduce.pipes.isjavarecordreader |
| hadoop.pipes.java.recordwriter | mapreduce.pipes.isjavarecordwriter |
| hadoop.pipes.java.reducer | mapreduce.pipes.isjavareducer |
| hadoop.pipes.partitioner | mapreduce.pipes.partitioner |
| heartbeat.recheck.interval | dfs.namenode.heartbeat.recheck-interval |
| io.bytes.per.checksum | dfs.bytes-per-checksum |
| io.sort.factor | mapreduce.task.io.sort.factor |
| io.sort.mb | mapreduce.task.io.sort.mb |
| io.sort.spill.percent | mapreduce.map.sort.spill.percent |
| jobclient.completion.poll.interval | mapreduce.client.completion.pollinterval |
| jobclient.output.filter | mapreduce.client.output.filter |
| jobclient.progress.monitor.poll.interval | mapreduce.client.progressmonitor.pollinterval |
| job.end.notification.url | mapreduce.job.end-notification.url |
| job.end.retry.attempts | mapreduce.job.end-notification.retry.attempts |
| job.end.retry.interval | mapreduce.job.end-notification.retry.interval |
| job.local.dir | mapreduce.job.local.dir |
| keep.failed.task.files | mapreduce.task.files.preserve.failedtasks |
| keep.task.files.pattern | mapreduce.task.files.preserve.filepattern |
| key.value.separator.in.input.line | mapreduce.input.keyvaluelinerecordreader.key.value.separator |
| local.cache.size | mapreduce.tasktracker.cache.local.size |
| map.input.file | mapreduce.map.input.file |
| map.input.length | mapreduce.map.input.length |
| map.input.start | mapreduce.map.input.start |
| map.output.key.field.separator | mapreduce.map.output.key.field.separator |
| map.output.key.value.fields.spec | mapreduce.fieldsel.map.output.key.value.fields.spec |
| mapred.acls.enabled | mapreduce.cluster.acls.enabled |
| mapred.binary.partitioner.left.offset | mapreduce.partition.binarypartitioner.left.offset |
| mapred.binary.partitioner.right.offset | mapreduce.partition.binarypartitioner.right.offset |
| mapred.cache.archives | mapreduce.job.cache.archives |
| mapred.cache.archives.timestamps | mapreduce.job.cache.archives.timestamps |
| mapred.cache.files | mapreduce.job.cache.files |
| mapred.cache.files.timestamps | mapreduce.job.cache.files.timestamps |
| mapred.cache.localArchives | mapreduce.job.cache.local.archives |
| mapred.cache.localFiles | mapreduce.job.cache.local.files |
| mapred.child.tmp | mapreduce.task.tmp.dir |
| mapred.cluster.average.blacklist.threshold | mapreduce.jobtracker.blacklist.average.threshold |
| mapred.cluster.map.memory.mb | mapreduce.cluster.mapmemory.mb |
| mapred.cluster.max.map.memory.mb | mapreduce.jobtracker.maxmapmemory.mb |
| mapred.cluster.max.reduce.memory.mb | mapreduce.jobtracker.maxreducememory.mb |
| mapred.cluster.reduce.memory.mb | mapreduce.cluster.reducememory.mb |
| mapred.committer.job.setup.cleanup.needed | mapreduce.job.committer.setup.cleanup.needed |
| mapred.compress.map.output | mapreduce.map.output.compress |
| mapred.data.field.separator | mapreduce.fieldsel.data.field.separator |
| mapred.debug.out.lines | mapreduce.task.debugout.lines |
| mapred.healthChecker.interval | mapreduce.tasktracker.healthchecker.interval |
| mapred.healthChecker.script.args | mapreduce.tasktracker.healthchecker.script.args |
| mapred.healthChecker.script.path | mapreduce.tasktracker.healthchecker.script.path |
| mapred.healthChecker.script.timeout | mapreduce.tasktracker.healthchecker.script.timeout |
| mapred.heartbeats.in.second | mapreduce.jobtracker.heartbeats.in.second |
| mapred.hosts.exclude | mapreduce.jobtracker.hosts.exclude.filename |
| mapred.hosts | mapreduce.jobtracker.hosts.filename |
| mapred.inmem.merge.threshold | mapreduce.reduce.merge.inmem.threshold |
| mapred.input.dir.formats | mapreduce.input.multipleinputs.dir.formats |
| mapred.input.dir.mappers | mapreduce.input.multipleinputs.dir.mappers |
| mapred.input.dir | mapreduce.input.fileinputformat.inputdir |
| mapred.input.pathFilter.class | mapreduce.input.pathFilter.class |
| mapred.jar | mapreduce.job.jar |
| mapred.job.classpath.archives | mapreduce.job.classpath.archives |
| mapred.job.classpath.files | mapreduce.job.classpath.files |
| mapred.job.id | mapreduce.job.id |
| mapred.jobinit.threads | mapreduce.jobtracker.jobinit.threads |
| mapred.job.map.memory.mb | mapreduce.map.memory.mb |
| mapred.job.name | mapreduce.job.name |
| mapred.job.priority | mapreduce.job.priority |
| mapred.job.queue.name | mapreduce.job.queuename |
| mapred.job.reduce.input.buffer.percent | mapreduce.reduce.input.buffer.percent |
| mapred.job.reduce.markreset.buffer.percent | mapreduce.reduce.markreset.buffer.percent |
| mapred.job.reduce.memory.mb | mapreduce.reduce.memory.mb |
| mapred.job.reduce.total.mem.bytes | mapreduce.reduce.memory.totalbytes |
| mapred.job.reuse.jvm.num.tasks | mapreduce.job.jvm.numtasks |
| mapred.job.shuffle.input.buffer.percent | mapreduce.reduce.shuffle.input.buffer.percent |
| mapred.job.shuffle.merge.percent | mapreduce.reduce.shuffle.merge.percent |
| mapred.job.tracker.handler.count | mapreduce.jobtracker.handler.count |
| mapred.job.tracker.history.completed.location | mapreduce.jobtracker.jobhistory.completed.location |
| mapred.job.tracker.http.address | mapreduce.jobtracker.http.address |
| mapred.jobtracker.instrumentation | mapreduce.jobtracker.instrumentation |
| mapred.jobtracker.job.history.block.size | mapreduce.jobtracker.jobhistory.block.size |
| mapred.job.tracker.jobhistory.lru.cache.size | mapreduce.jobtracker.jobhistory.lru.cache.size |
| mapred.job.tracker | mapreduce.jobtracker.address |
| mapred.jobtracker.maxtasks.per.job | mapreduce.jobtracker.maxtasks.perjob |
| mapred.job.tracker.persist.jobstatus.active | mapreduce.jobtracker.persist.jobstatus.active |
| mapred.job.tracker.persist.jobstatus.dir | mapreduce.jobtracker.persist.jobstatus.dir |
| mapred.job.tracker.persist.jobstatus.hours | mapreduce.jobtracker.persist.jobstatus.hours |
| mapred.jobtracker.restart.recover | mapreduce.jobtracker.restart.recover |
| mapred.job.tracker.retiredjobs.cache.size | mapreduce.jobtracker.retiredjobs.cache.size |
| mapred.job.tracker.retire.jobs | mapreduce.jobtracker.retirejobs |
| mapred.jobtracker.taskalloc.capacitypad | mapreduce.jobtracker.taskscheduler.taskalloc.capacitypad |
| mapred.jobtracker.taskScheduler | mapreduce.jobtracker.taskscheduler |
| mapred.jobtracker.taskScheduler.maxRunningTasksPerJob | mapreduce.jobtracker.taskscheduler.maxrunningtasks.perjob |
| mapred.join.expr | mapreduce.join.expr |
| mapred.join.keycomparator | mapreduce.join.keycomparator |
| mapred.lazy.output.format | mapreduce.output.lazyoutputformat.outputformat |
| mapred.line.input.format.linespermap | mapreduce.input.lineinputformat.linespermap |
| mapred.linerecordreader.maxlength | mapreduce.input.linerecordreader.line.maxlength |
| mapred.local.dir | mapreduce.cluster.local.dir |
| mapred.local.dir.minspacekill | mapreduce.tasktracker.local.dir.minspacekill |
| mapred.local.dir.minspacestart | mapreduce.tasktracker.local.dir.minspacestart |
| mapred.map.child.env | mapreduce.map.env |
| mapred.map.child.java.opts | mapreduce.map.java.opts |
| mapred.map.child.log.level | mapreduce.map.log.level |
| mapred.map.max.attempts | mapreduce.map.maxattempts |
| mapred.map.output.compression.codec | mapreduce.map.output.compress.codec |
| mapred.mapoutput.key.class | mapreduce.map.output.key.class |
| mapred.mapoutput.value.class | mapreduce.map.output.value.class |
| mapred.mapper.regex.group | mapreduce.mapper.regexmapper..group |
| mapred.mapper.regex | mapreduce.mapper.regex |
| mapred.map.task.debug.script | mapreduce.map.debug.script |
| mapred.map.tasks | mapreduce.job.maps |
| mapred.map.tasks.speculative.execution | mapreduce.map.speculative |
| mapred.max.map.failures.percent | mapreduce.map.failures.maxpercent |
| mapred.max.reduce.failures.percent | mapreduce.reduce.failures.maxpercent |
| mapred.max.split.size | mapreduce.input.fileinputformat.split.maxsize |
| mapred.max.tracker.blacklists | mapreduce.jobtracker.tasktracker.maxblacklists |
| mapred.max.tracker.failures | mapreduce.job.maxtaskfailures.per.tracker |
| mapred.merge.recordsBeforeProgress | mapreduce.task.merge.progress.records |
| mapred.min.split.size | mapreduce.input.fileinputformat.split.minsize |
| mapred.min.split.size.per.node | mapreduce.input.fileinputformat.split.minsize.per.node |
| mapred.min.split.size.per.rack | mapreduce.input.fileinputformat.split.minsize.per.rack |
| mapred.output.compression.codec | mapreduce.output.fileoutputformat.compress.codec |
| mapred.output.compression.type | mapreduce.output.fileoutputformat.compress.type |
| mapred.output.compress | mapreduce.output.fileoutputformat.compress |
| mapred.output.dir | mapreduce.output.fileoutputformat.outputdir |
| mapred.output.key.class | mapreduce.job.output.key.class |
| mapred.output.key.comparator.class | mapreduce.job.output.key.comparator.class |
| mapred.output.value.class | mapreduce.job.output.value.class |
| mapred.output.value.groupfn.class | mapreduce.job.output.group.comparator.class |
| mapred.permissions.supergroup | mapreduce.cluster.permissions.supergroup |
| mapred.pipes.user.inputformat | mapreduce.pipes.inputformat |
| mapred.reduce.child.env | mapreduce.reduce.env |
| mapred.reduce.child.java.opts | mapreduce.reduce.java.opts |
| mapred.reduce.child.log.level | mapreduce.reduce.log.level |
| mapred.reduce.max.attempts | mapreduce.reduce.maxattempts |
| mapred.reduce.parallel.copies | mapreduce.reduce.shuffle.parallelcopies |
| mapred.reduce.slowstart.completed.maps | mapreduce.job.reduce.slowstart.completedmaps |
| mapred.reduce.task.debug.script | mapreduce.reduce.debug.script |
| mapred.reduce.tasks | mapreduce.job.reduces |
| mapred.reduce.tasks.speculative.execution | mapreduce.reduce.speculative |
| mapred.seqbinary.output.key.class | mapreduce.output.seqbinaryoutputformat.key.class |
| mapred.seqbinary.output.value.class | mapreduce.output.seqbinaryoutputformat.value.class |
| mapred.shuffle.connect.timeout | mapreduce.reduce.shuffle.connect.timeout |
| mapred.shuffle.read.timeout | mapreduce.reduce.shuffle.read.timeout |
| mapred.skip.attempts.to.start.skipping | mapreduce.task.skip.start.attempts |
| mapred.skip.map.auto.incr.proc.count | mapreduce.map.skip.proc-count.auto-incr |
| mapred.skip.map.max.skip.records | mapreduce.map.skip.maxrecords |
| mapred.skip.on | mapreduce.job.skiprecords |
| mapred.skip.out.dir | mapreduce.job.skip.outdir |
| mapred.skip.reduce.auto.incr.proc.count | mapreduce.reduce.skip.proc-count.auto-incr |
| mapred.skip.reduce.max.skip.groups | mapreduce.reduce.skip.maxgroups |
| mapred.speculative.execution.slowNodeThreshold | mapreduce.job.speculative.slownodethreshold |
| mapred.speculative.execution.slowTaskThreshold | mapreduce.job.speculative.slowtaskthreshold |
| mapred.speculative.execution.speculativeCap | mapreduce.job.speculative.speculativecap |
| mapred.submit.replication | mapreduce.client.submit.file.replication |
| mapred.system.dir | mapreduce.jobtracker.system.dir |
| mapred.task.cache.levels | mapreduce.jobtracker.taskcache.levels |
| mapred.task.id | mapreduce.task.attempt.id |
| mapred.task.is.map | mapreduce.task.ismap |
| mapred.task.partition | mapreduce.task.partition |
| mapred.task.profile | mapreduce.task.profile |
| mapred.task.profile.maps | mapreduce.task.profile.maps |
| mapred.task.profile.params | mapreduce.task.profile.params |
| mapred.task.profile.reduces | mapreduce.task.profile.reduces |
| mapred.task.timeout | mapreduce.task.timeout |
| mapred.tasktracker.dns.interface | mapreduce.tasktracker.dns.interface |
| mapred.tasktracker.dns.nameserver | mapreduce.tasktracker.dns.nameserver |
| mapred.tasktracker.events.batchsize | mapreduce.tasktracker.events.batchsize |
| mapred.tasktracker.expiry.interval | mapreduce.jobtracker.expire.trackers.interval |
| mapred.task.tracker.http.address | mapreduce.tasktracker.http.address |
| mapred.tasktracker.indexcache.mb | mapreduce.tasktracker.indexcache.mb |
| mapred.tasktracker.instrumentation | mapreduce.tasktracker.instrumentation |
| mapred.tasktracker.map.tasks.maximum | mapreduce.tasktracker.map.tasks.maximum |
| mapred.tasktracker.memory\_calculator\_plugin | mapreduce.tasktracker.resourcecalculatorplugin |
| mapred.tasktracker.memorycalculatorplugin | mapreduce.tasktracker.resourcecalculatorplugin |
| mapred.tasktracker.reduce.tasks.maximum | mapreduce.tasktracker.reduce.tasks.maximum |
| mapred.task.tracker.report.address | mapreduce.tasktracker.report.address |
| mapred.task.tracker.task-controller | mapreduce.tasktracker.taskcontroller |
| mapred.tasktracker.taskmemorymanager.monitoring-interval | mapreduce.tasktracker.taskmemorymanager.monitoringinterval |
| mapred.tasktracker.tasks.sleeptime-before-sigkill | mapreduce.tasktracker.tasks.sleeptimebeforesigkill |
| mapred.temp.dir | mapreduce.cluster.temp.dir |
| mapred.text.key.comparator.options | mapreduce.partition.keycomparator.options |
| mapred.text.key.partitioner.options | mapreduce.partition.keypartitioner.options |
| mapred.textoutputformat.separator | mapreduce.output.textoutputformat.separator |
| mapred.tip.id | mapreduce.task.id |
| mapreduce.combine.class | mapreduce.job.combine.class |
| mapreduce.inputformat.class | mapreduce.job.inputformat.class |
| mapreduce.job.counters.limit | mapreduce.job.counters.max |
| mapreduce.jobtracker.permissions.supergroup | mapreduce.cluster.permissions.supergroup |
| mapreduce.map.class | mapreduce.job.map.class |
| mapreduce.outputformat.class | mapreduce.job.outputformat.class |
| mapreduce.partitioner.class | mapreduce.job.partitioner.class |
| mapreduce.reduce.class | mapreduce.job.reduce.class |
| mapred.used.genericoptionsparser | mapreduce.client.genericoptionsparser.used |
| mapred.userlog.limit.kb | mapreduce.task.userlog.limit.kb |
| mapred.userlog.retain.hours | mapreduce.job.userlog.retain.hours |
| mapred.working.dir | mapreduce.job.working.dir |
| mapred.work.output.dir | mapreduce.task.output.dir |
| min.num.spills.for.combine | mapreduce.map.combine.minspills |
| reduce.output.key.value.fields.spec | mapreduce.fieldsel.reduce.output.key.value.fields.spec |
| security.job.submission.protocol.acl | security.job.client.protocol.acl |
| security.task.umbilical.protocol.acl | security.job.task.protocol.acl |
| sequencefile.filter.class | mapreduce.input.sequencefileinputfilter.class |
| sequencefile.filter.frequency | mapreduce.input.sequencefileinputfilter.frequency |
| sequencefile.filter.regex | mapreduce.input.sequencefileinputfilter.regex |
| session.id | dfs.metrics.session-id |
| slave.host.name | dfs.datanode.hostname |
| slave.host.name | mapreduce.tasktracker.host.name |
| tasktracker.contention.tracking | mapreduce.tasktracker.contention.tracking |
| tasktracker.http.threads | mapreduce.tasktracker.http.threads |
| topology.node.switch.mapping.impl | net.topology.node.switch.mapping.impl |
| topology.script.file.name | net.topology.script.file.name |
| topology.script.number.args | net.topology.script.number.args |
| user.name | mapreduce.job.user.name |
| webinterface.private.actions | mapreduce.jobtracker.webinterface.trusted |
| yarn.app.mapreduce.yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts | yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts |

The following table lists additional changes to some configuration properties:

| **Deprecated property name** | **New property name** |
|:---- |:---- |
| mapred.create.symlink | NONE - symlinking is always on |
| mapreduce.job.cache.symlink.create | NONE - symlinking is always on |


