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
package org.apache.storm.s3.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.s3.S3Output;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.FileNameFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;
import org.apache.storm.s3.trident.state.S3StateFactory;
import org.apache.storm.s3.trident.state.S3Updater;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

public class TridentFileTopology {

    public static StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence", "key"), 1000, new Values("the cow jumped over the moon", 1l),
                new Values("the man went to the store and bought some candy", 2l), new Values("four score and seven years ago", 3l),
                new Values("how many apples can you eat", 4l), new Values("to be or not to be the person", 5l));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        Fields fields = new Fields("sentence", "key");

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("trident")
                .withPrefix("trident")
                .withExtension(".txt");

        RecordFormat recordFormat = new DelimitedRecordFormat()
                .withFields(new Fields("sentence"));

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.MB);

        S3Output s3 = new S3Output()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withBucketName("trident-state")
                .withRotationPolicy(rotationPolicy);


        StateFactory factory = new S3StateFactory(s3);
        stream.partitionPersist(factory, fields, new S3Updater(), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("S3-Trident", conf, buildTopology());
            Thread.sleep(120 * 1000);
        } else if (args.length == 1) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[1], conf, buildTopology());
        } else {
            System.out.println("Usage: TridentFileTopology [topology name]");
        }
    }
}
