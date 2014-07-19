package org.apache.storm.s3.trident.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import org.apache.storm.s3.S3Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class S3State implements State {

    public static final Logger LOG = LoggerFactory.getLogger(S3State.class);
    private S3Output s3;

    S3State(S3Output s3) {
        this.s3 = s3;
    }

    void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        try {
            String identifier = String.valueOf(partitionIndex);
            this.s3.withIdentifier(identifier);
            this.s3.prepare(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void beginCommit(Long txId) {

    }

    @Override
    public void commit(Long txId) {

    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector) {
        try {
            for (TridentTuple tuple : tuples) {
                byte[] bytes = s3.getRecordFormat().format(tuple);
                s3.write(bytes);
            }
        } catch (IOException e) {
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }
}
