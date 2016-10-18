package com.automattic.elasticsearch.plugin;

import com.automattic.elasticsearch.statsd.StatsdService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class StatsdPlugin extends Plugin {

    public static final Setting<TimeValue> EVERY_S = Setting.timeSetting("metrics.statsd.every", TimeValue.timeValueMinutes(1), Setting.Property.NodeScope);
    public static final Setting<String> HOST_S = new Setting<>("metrics.statsd.host", "localhost", Function.identity(), Setting.Property.NodeScope);
    public static final Setting<Integer> PORT_S = Setting.intSetting("metrics.statsd.port", 8125, 1, 65535, Setting.Property.NodeScope);
    public static final Setting<Boolean> REPORT_NODE_INDICES_S = Setting.boolSetting("metrics.statsd.report.node_indices", false, Setting.Property.NodeScope);
    public static final Setting<Boolean> REPORT_INDICES_S = Setting.boolSetting("metrics.statsd.report.indices", true, Setting.Property.NodeScope);
    public static final Setting<Boolean> REPORT_SHARDS_S = Setting.boolSetting("metrics.statsd.report.shards", false, Setting.Property.NodeScope);
    public static final Setting<Boolean> REPORT_FS_DETAILS_S = Setting.boolSetting("metrics.statsd.report.fs_details", false, Setting.Property.NodeScope);
    public static final Setting<String> NODE_NAME_S = new Setting<>("metrics.statsd.node_name", "", Function.identity(), Setting.Property.NodeScope);
    public static final Setting<String> PREFIX_S = new Setting<>("metrics.statsd.prefix", "", Function.identity(), Setting.Property.NodeScope);

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                EVERY_S,
                HOST_S,
                PORT_S,
                REPORT_NODE_INDICES_S,
                REPORT_INDICES_S,
                REPORT_SHARDS_S,
                REPORT_FS_DETAILS_S,
                NODE_NAME_S,
                PREFIX_S
        );
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
                Collection<Class<? extends LifecycleComponent>> list = new ArrayList<>(1);
        list.add(StatsdService.class);
        return list;
    }
}
