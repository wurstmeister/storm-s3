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
package org.apache.storm.s3.output;

import backtype.storm.tuple.Fields;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.FileNameFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;

import java.util.List;
import java.util.Map;

public class S3Configuration {

    public static final String PREFIX = "PREFIX";
    public static final String EXTENSION = "EXTENSION";
    public static final String PATH = "PATH";
    public static final String OUTPUT_FIELDS = "OUTPUT_FIELDS";
    public static final String ROTATION_SIZE = "ROTATION_SIZE";
    public static final String ROTATION_UNIT = "ROTATION_UNIT";
    public static final String BUCKET_NAME = "BUCKET_NAME";

    private FileRotationPolicy rotationPolicy;
    private FileNameFormat fileNameFormat;
    private RecordFormat format;
    private String bucketName;


    public S3Configuration(Map conf) {
        String prefix = (String) conf.get(PREFIX);
        String extension = (String) conf.get(EXTENSION);
        String path = (String) conf.get(PATH);
        fileNameFormat = new DefaultFileNameFormat().withExtension(extension).withPath(path).withPrefix(prefix);
        Fields fields = null;
        if (conf.containsKey(OUTPUT_FIELDS)) {
            List fieldsList = (List) conf.get(OUTPUT_FIELDS);
            fields = new Fields(fieldsList);
        }
        format = new DelimitedRecordFormat().withFields(fields);
        Float rotationSize = ((Double) conf.get(ROTATION_SIZE)).floatValue();
        FileSizeRotationPolicy.Units rotationUnit = FileSizeRotationPolicy.Units.valueOf((String) conf.get(ROTATION_UNIT));
        rotationPolicy = new FileSizeRotationPolicy(rotationSize, rotationUnit);

        if (conf.containsKey(BUCKET_NAME)) {
            bucketName = (String) conf.get(BUCKET_NAME);
        }
    }

    public FileNameFormat getFileNameFormat() {
        return fileNameFormat;
    }

    public FileRotationPolicy getRotationPolicy() {
        return rotationPolicy;
    }

    public RecordFormat getRecordFormat() {
        return format;
    }

    public String getBucketName() {
        return bucketName;
    }
}
