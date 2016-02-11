package org.elasticsearch.module.statsd.test;

import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugin.statsd.StatsdPlugin;

import java.io.IOException;

public class NodeTestHelper
{

	public static Node createNode(String clusterName, final int numberOfShards, int statsdPort, String refreshInterval)
		throws IOException
	{
		Settings.Builder settingsBuilder = Settings.settingsBuilder();

		settingsBuilder.put("cluster.name", clusterName);
		settingsBuilder.put("index.number_of_shards", numberOfShards);
		settingsBuilder.put("index.number_of_replicas", 1);

		settingsBuilder.put("metrics.statsd.host", "localhost");
		settingsBuilder.put("metrics.statsd.port", statsdPort);
		settingsBuilder.put("metrics.statsd.every", refreshInterval);

		settingsBuilder.put("path.home", "target/test-classes");
		settingsBuilder.put("path.conf", "target/test-classes/config");

		// Load statsd plugin
		settingsBuilder.put("plugin.types", StatsdPlugin.class.getName());

		LogConfigurator.configure(settingsBuilder.build(), true);

		return NodeBuilder.nodeBuilder().settings(settingsBuilder.build()).node();
	}
}
