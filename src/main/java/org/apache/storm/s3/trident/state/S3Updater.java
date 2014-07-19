package org.apache.storm.s3.trident.state;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class S3Updater extends BaseStateUpdater<S3State> {
    @Override
    public void updateState(S3State state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }
}
