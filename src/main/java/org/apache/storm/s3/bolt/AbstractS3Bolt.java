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
package org.apache.storm.s3.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.storm.s3.bolt.format.FileNameFormat;
import org.apache.storm.s3.bolt.rotation.FileRotationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractS3Bolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractS3Bolt.class);

    protected OutputCollector collector;

    protected FileRotationPolicy rotationPolicy;
    protected FileNameFormat fileNameFormat;
    protected int rotation = 0;

    abstract void closeOutputFile() throws IOException;

    abstract void createOutputFile() throws IOException;

    abstract void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException;

    protected void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        closeOutputFile();
        this.rotation++;
        createOutputFile();
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }

    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        if (this.rotationPolicy == null) {
            throw new IllegalStateException("RotationPolicy must be specified.");
        }
        this.collector = collector;
        this.fileNameFormat.prepare(conf, topologyContext);

        try {
            doPrepare(conf, topologyContext, collector);
            createOutputFile();
        } catch (Exception e) {
            throw new RuntimeException("Error preparing S3Bolt: " + e.getMessage(), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }


}
