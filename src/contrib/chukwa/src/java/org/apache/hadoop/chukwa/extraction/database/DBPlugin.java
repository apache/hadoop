package org.apache.hadoop.chukwa.extraction.database;

import org.apache.hadoop.io.SequenceFile;

public interface DBPlugin
{
	void process(SequenceFile.Reader reader)
	throws DBException;
}
