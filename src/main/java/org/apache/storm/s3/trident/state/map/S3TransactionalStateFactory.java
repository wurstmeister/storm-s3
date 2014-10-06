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

import backtype.storm.task.IMetricsContext;
import org.apache.storm.s3.output.trident.DefaultFileOutputFactory;
import org.apache.storm.s3.output.trident.FileOutputFactory;
import org.apache.storm.s3.output.trident.S3TransactionalOutputFactory;
import storm.trident.state.State;
import storm.trident.state.map.TransactionalMap;

import storm.trident.state.StateFactory;
import java.util.Map;

public class S3TransactionalStateFactory implements StateFactory {

    private FileOutputFactory fileOutputFactory = new DefaultFileOutputFactory();
    private S3TransactionalOutputFactory factory;

    public S3TransactionalStateFactory(S3TransactionalOutputFactory factory, FileOutputFactory fileOutputFactory) {
        this.fileOutputFactory = fileOutputFactory;
        this.factory = factory;
    }

    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return TransactionalMap.build(new S3MapState(conf, factory, fileOutputFactory));
    }


}
