package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.common.Abortable;

public abstract class AbortableRecordWriter<K, V> implements RecordWriter<K, V>, Abortable {
}
