package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.ClientFactory;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;

/**
 * Factory responsible for local job runner clients.
 *
 */
public class LocalClientFactory extends ClientFactory {

	@Override
	protected ClientProtocol createClient(Configuration conf)
			throws IOException {
		return new LocalJobRunner(conf);
	}
}