package com.automattic.elasticsearch.statsd;

import org.elasticsearch.indices.NodeIndicesStats;

public class StatsdReporterNodeIndicesStats extends StatsdReporterIndexStats {

    private final NodeIndicesStats nodeIndicesStats;
    private final String nodeName;

    public StatsdReporterNodeIndicesStats(NodeIndicesStats nodeIndicesStats, String nodeName) {
        this.nodeIndicesStats = nodeIndicesStats;
        this.nodeName = nodeName;
    }

    public void run() {
        try {
            String prefix = this.buildMetricName("node." + this.nodeName + ".indices");
            this.sendDocsStats(prefix + ".docs", this.nodeIndicesStats.getDocs());
            this.sendStoreStats(prefix + ".store", this.nodeIndicesStats.getStore());
            this.sendIndexingStats(prefix + ".indexing", this.nodeIndicesStats.getIndexing());
            this.sendGetStats(prefix + ".get", this.nodeIndicesStats.getGet());
            this.sendSearchStats(prefix + ".search", this.nodeIndicesStats.getSearch());
            this.sendMergeStats(prefix + ".merges", this.nodeIndicesStats.getMerge());
            this.sendRefreshStats(prefix + ".refresh", this.nodeIndicesStats.getRefresh());
            this.sendFlushStats(prefix + ".flush", this.nodeIndicesStats.getFlush());
            this.sendFielddataCacheStats(prefix + ".fielddata", this.nodeIndicesStats.getFieldData());
            this.sendPercolateStats(prefix + ".percolate", this.nodeIndicesStats.getPercolate());
            this.sendCompletionStats(prefix + ".completion", this.nodeIndicesStats.getCompletion());
            this.sendSegmentsStats(prefix + ".segments", this.nodeIndicesStats.getSegments());
            this.sendQueryCacheStats(prefix + ".query_cache", this.nodeIndicesStats.getQueryCache());
            this.sendRequestCacheStats(prefix + ".request_cache", this.nodeIndicesStats.getRequestCache());
        } catch (Exception e) {
            this.logException(e);
        }
    }
}
