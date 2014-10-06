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
package org.apache.storm.s3.trident.state.map;

import org.apache.storm.s3.output.trident.FileOutputFactory;
import org.apache.storm.s3.output.trident.S3TransactionalOutput;
import org.apache.storm.s3.output.trident.S3TransactionalOutputFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3MapState<T> implements IBackingMap<TransactionalValue<T>> {

    public static final Logger LOG = LoggerFactory.getLogger(S3MapState.class);
    private Map conf, writers;
    private S3TransactionalOutputFactory transactionalOutputFactory;
    private FileOutputFactory<T> fileOutputFactory;


    public S3MapState(Map conf, S3TransactionalOutputFactory transactionalOutputFactory, FileOutputFactory<T> fileOutputFactory) {
        writers = new HashMap();
        this.conf = conf;
        this.transactionalOutputFactory = transactionalOutputFactory;
        this.fileOutputFactory = fileOutputFactory;
    }

    /**
     * Give me the latest transactional values that you have for the keys given
     *
     * @param keys
     * @return
     */
    public List<TransactionalValue<T>> multiGet(List<List<Object>> keys) {
        List<TransactionalValue<T>> result = new ArrayList(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            TridentTuple keyTuple = (TridentTuple) keys.get(i);
            T key = (T) keyTuple.get(0);
            S3TransactionalOutput output = outputFor(key);
            result.add(new TransactionalValue(output.getTransactionId(), null));
        }

        return result;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<TransactionalValue<T>> vals) {
        for (int i = 0; i < keys.size(); i++) {
            TridentTuple keyTuple = (TridentTuple) keys.get(i);
            TransactionalValue transactionalValue = (TransactionalValue) vals.get(i);
            Long txid = transactionalValue.getTxid();
            T key = (T) keyTuple.get(0);
            S3TransactionalOutput output = outputFor(key);
            List tuples = (List) transactionalValue.getVal();
            try {
                output.write(key, tuples, txid);
            } catch (IOException e) {
                LOG.error("Error while writing tuples for key {}", e, key);
            }
        }

    }

    private S3TransactionalOutput outputFor(T key) {
        S3TransactionalOutput<T> output = (S3TransactionalOutput) writers.get(key);
        if (output == null) {
            output = transactionalOutputFactory.build(key, conf, fileOutputFactory);
            try {
                output.prepare(conf);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
            writers.put(key, output);
        }
        return output;
    }
}
