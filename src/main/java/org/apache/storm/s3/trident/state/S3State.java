/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.trident.state;

import backtype.storm.topology.FailedException;
import org.apache.storm.s3.output.trident.S3TransactionalOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.List;

public class S3State implements State {

    public static final Logger LOG = LoggerFactory.getLogger(S3State.class);
    private S3TransactionalOutput s3;
    private Long txId;

    S3State(S3TransactionalOutput s3) {
        this.s3 = s3;
    }

    @Override
    public void beginCommit(Long txId) {
        this.txId = txId;
    }

    @Override
    public void commit(Long txId) {

    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector) {
        try {
            s3.write(tuples, txId);
        } catch (IOException e) {
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }
}
