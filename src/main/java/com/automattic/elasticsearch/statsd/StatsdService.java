package com.automattic.elasticsearch.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.service.NodeService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatsdService extends AbstractLifecycleComponent<StatsdService> {

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final NodeService nodeService;
    private final String statsdHost;
    private final Integer statsdPort;
    private final TimeValue statsdRefreshInternal;
    private final String statsdPrefix;
    private final Boolean statsdReportNodeIndices;
    private final Boolean statsdReportIndices;
    private final Boolean statsdReportShards;
    private final Boolean statsdReportFsDetails;
    private final String[] tags;
    private final StatsDClient statsdClient;

    private final Thread statsdReporterThread;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Inject
    public StatsdService(Settings settings, Client client, ClusterService clusterService, IndicesService indicesService, NodeService nodeService) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeService = nodeService;
        this.statsdRefreshInternal = settings.getAsTime(
                "metrics.statsd.every", TimeValue.timeValueMinutes(1)
        );
        this.statsdHost = settings.get(
                "metrics.statsd.host"
        );
        this.statsdPort = settings.getAsInt(
                "metrics.statsd.port", 8125
        );
        this.statsdPrefix = settings.get(
                "metrics.statsd.prefix", "elasticsearch" + "." + settings.get("cluster.name")
        );
        this.statsdReportNodeIndices = settings.getAsBoolean(
                "metrics.statsd.report.node_indices", false
        );
        this.statsdReportIndices = settings.getAsBoolean(
                "metrics.statsd.report.indices", true
        );
        this.statsdReportShards = settings.getAsBoolean(
                "metrics.statsd.report.shards", false
        );
        this.statsdReportFsDetails = settings.getAsBoolean(
                "metrics.statsd.report.fs_details", false
        );
        this.tags = settings.getAsArray(
                "metrics.statsd.report.tags", new String[] {}
        );

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        this.statsdClient = AccessController.doPrivileged(new PrivilegedAction<StatsDClient>() {
            @Override
            public StatsDClient run() {
                return new NonBlockingStatsDClient(
                        StatsdService.this.statsdPrefix,
                        StatsdService.this.statsdHost,
                        StatsdService.this.statsdPort,
                        StatsdService.this.tags
                    );
            }
        });

        this.statsdReporterThread = EsExecutors
                .daemonThreadFactory(this.settings, "statsd_reporter")
                .newThread(new StatsdReporterThread());
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (this.statsdHost != null && this.statsdHost.length() > 0) {
            this.statsdReporterThread.start();
            this.logger.info(
                    "StatsD reporting triggered every [{}] to host [{}:{}] with metric prefix [{}]",
                    this.statsdRefreshInternal, this.statsdHost, this.statsdPort, this.statsdPrefix
            );
        } else {
            this.logger.error(
                    "StatsD reporting disabled, no StatsD host configured"
            );
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        doClose();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        if(this.closed.compareAndSet(false, true)) {
            this.statsdReporterThread.interrupt();
            this.logger.info("StatsD reporter stopped");
        }
    }

    public class StatsdReporterThread implements Runnable {

        @Override
        public void run() {
            try {
                while (!StatsdService.this.closed.get()) {
                    DiscoveryNode node = StatsdService.this.clusterService.localNode();
                    ClusterState state = StatsdService.this.clusterService.state();
                    boolean isClusterStarted = StatsdService.this.clusterService
                            .lifecycleState()
                            .equals(Lifecycle.State.STARTED);

                    if (node != null && state != null && isClusterStarted) {

                        // Report node stats -- runs for all nodes
                        try {
                            StatsdReporter nodeStatsReporter = new StatsdReporterNodeStats(
                                    StatsdService.this.nodeService.stats(
                                            new CommonStatsFlags().clear(), // indices
                                            true, // os
                                            true, // process
                                            true, // jvm
                                            true, // threadPool
                                            true, // network
                                            true, // fs
                                            true, // transport
                                            true, // http
                                            false // circuitBreaker
                                    ),
                                    StatsdService.this.statsdReportFsDetails
                            );
                            nodeStatsReporter
                                    .setStatsDClient(StatsdService.this.statsdClient)
                                    .run();
                        } catch (Exception e) {
                            StatsdService.this.logger.error("Unable to send node stats", e);
                        }

                        // Maybe report index stats per node
                        if (StatsdService.this.statsdReportNodeIndices && node.isDataNode()) {
                            try {
                                StatsdReporter nodeIndicesStatsReporter = new StatsdReporterNodeIndicesStats(
                                        StatsdService.this.indicesService.stats(
                                                false // includePrevious
                                        )
                                );
                                nodeIndicesStatsReporter
                                        .setStatsDClient(StatsdService.this.statsdClient)
                                        .run();
                            } catch (Exception e) {
                                StatsdService.this.logger.error("Unable to send node indices stats", e);
                            }
                        }

                        // Master node is the only one allowed to send cluster wide sums / stats
                        if (state.nodes().localNodeMaster()) {
                            try {
                                StatsdReporter indicesReporter = new StatsdReporterIndices(
                                        StatsdService.this.client
                                                .admin()        // AdminClient
                                                .indices()      // IndicesAdminClient
                                                .prepareStats() // IndicesStatsRequestBuilder
                                                .all()          // IndicesStatsRequestBuilder
                                                .get(),         // IndicesStatsResponse,
                                        StatsdService.this.client
                                                .admin()        // AdminClient
                                                .cluster()      // IndicesAdminClient
                                                .prepareHealth() // IndicesStatsRequestBuilder
                                                .get(),         // IndicesStatsResponse
                                        StatsdService.this.statsdReportIndices,
                                        StatsdService.this.statsdReportShards
                                );
                                indicesReporter
                                        .setStatsDClient(StatsdService.this.statsdClient)
                                        .run();
                            } catch (Exception e) {
                                StatsdService.this.logger.error("Unable to send cluster wide stats", e);
                            }
                        }
                    }

                    try {
                        Thread.sleep(StatsdService.this.statsdRefreshInternal.millis());
                    } catch (InterruptedException e1) {
                        continue;
                    }
                }
            } catch (Exception e) {
                StatsdService.this.logger.error("Exception thrown from the event loop of StatsdReporterThread", e);
            }

            StatsdService.this.logger.error("Exiting StatsdReporterThread");
        }
    }
}
