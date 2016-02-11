package org.elasticsearch.plugin.statsd;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.service.statsd.StatsdService;

import java.util.ArrayList;
import java.util.Collection;

public class StatsdPlugin extends Plugin
{

	public String name()
	{
		return "statsd";
	}

	public String description()
	{
		return "StatsD Monitoring Plugin";
	}

	@Override
	public Collection<Class<? extends LifecycleComponent>> nodeServices() {
		Collection<Class<? extends LifecycleComponent>> list = new ArrayList<>(1);
		list.add(StatsdService.class);
		return list;
	}

}
