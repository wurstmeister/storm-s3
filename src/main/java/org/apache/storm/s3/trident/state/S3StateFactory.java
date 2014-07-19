package org.apache.storm.s3.trident.state;

import backtype.storm.task.IMetricsContext;
import org.apache.storm.s3.S3Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class S3StateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S3StateFactory.class);
    private S3Output s3;

    public S3StateFactory(S3Output s3) {
        this.s3 = s3;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        S3State state = new S3State(this.s3);
        state.prepare(conf, metrics, partitionIndex, numPartitions);
        return state;
    }
}
