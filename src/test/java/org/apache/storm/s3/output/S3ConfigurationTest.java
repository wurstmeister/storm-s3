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

import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.FooBarFileNameFormat;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;
import org.junit.Test;

import java.util.HashMap;
import static org.junit.Assert.*;


public class S3ConfigurationTest {

    /**
     * Ensure we get the default <Code>FileSizeRotationPolicy</Code> when no config is provided.
     * @throws Exception
     */
    @Test
    public void testDefaultRotationPolicy() throws Exception {

        HashMap config = new HashMap();
        S3Configuration testConf = new S3Configuration(config);

        assertNotNull(testConf.getRotationPolicy());
        assertEquals(S3Configuration.DEFAULT_ROTATION_POLICY, testConf.getRotationPolicy());
    }

    /**
     * Ensure we get non default <Code>FileSizeRotationPolicy</Code> when the correct config is set.
     * @throws Exception
     */
    @Test
    public void testCustomRotationPolicy() throws Exception {

        HashMap config = new HashMap();

        config.put(S3Configuration.ROTATION_UNIT, "KB");
        config.put(S3Configuration.ROTATION_SIZE, 15.0);
        final FileSizeRotationPolicy policy = new FileSizeRotationPolicy(15.0F, FileSizeRotationPolicy.Units.KB);

        S3Configuration testConf = new S3Configuration(config);
        FileSizeRotationPolicy testConfFileSizePolicy = (FileSizeRotationPolicy)testConf.getRotationPolicy();

        assertFalse("Ensure not set to default", S3Configuration.DEFAULT_ROTATION_POLICY.getMaxBytes() == testConfFileSizePolicy.getMaxBytes());
        assertEquals("Ensure set correctly", policy.getMaxBytes(), testConfFileSizePolicy.getMaxBytes());

    }


    /**
     * Ensure we get the default <Code>FileNameFormat</Code> if <Code>S3Configuration.FILE_NAME_FORMAT_CLASS</Code> is
     * not specified.
     *
     * @throws Exception
     */
    @Test
    public void testDefaultFileNameFormat() throws Exception {

        HashMap config = new HashMap();
        S3Configuration testConf = new S3Configuration(config);

        assertNotNull(testConf.getFileNameFormat());
        assertTrue(testConf.getFileNameFormat() instanceof DefaultFileNameFormat);
    }

    /**
     * Ensure we get the default <Code>FileNameFormat</Code> if an invalid
     * <Code>S3Configuration.FILE_NAME_FORMAT_CLASS</Code> is specified.
     *
     * @throws Exception
     */
    @Test
    public void testBadNonDefaultFileNameFormat() throws Exception {

        HashMap config = new HashMap();
        config.put(S3Configuration.FILE_NAME_FORMAT_CLASS, "com.foo.bar.NotAValidClass");
        S3Configuration testConf = new S3Configuration(config);

        assertNotNull(testConf.getFileNameFormat());
    }

    /**
     * Ensure we get the the correct <Code>FileNameFormat</Code> if a valid
     * <Code>S3Configuration.FILE_NAME_FORMAT_CLASS</Code> is specified.
     *
     * @throws Exception
     */
    @Test
    public void testNonDefaultFileNameFormat() throws Exception {

        HashMap config = new HashMap();

        config.put(S3Configuration.FILE_NAME_FORMAT_CLASS, "org.apache.storm.s3.format.FooBarFileNameFormat");
        S3Configuration testConf = new S3Configuration(config);

        assertNotNull(testConf.getFileNameFormat());
        assertTrue(testConf.getFileNameFormat() instanceof FooBarFileNameFormat);
    }
}
