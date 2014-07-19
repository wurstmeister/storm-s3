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
package org.apache.storm.s3.rotation;

import java.util.concurrent.TimeUnit;

/**
 * The granularity of this policy depends on the frequency with which
 * tuples are written
 */
public class TimedRotationPolicy implements FileRotationPolicy {
    private long lastRotation;
    private long interval;

    public TimedRotationPolicy(long count, TimeUnit units) {
        this.interval = units.toMillis(count);
        lastRotation = System.currentTimeMillis();
    }

    /**
     * Called for every tuple the S3Bolt executes.
     *
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    @Override
    public boolean mark(long offset) {
        long elapsed = System.currentTimeMillis() - lastRotation;
        return elapsed > interval;
    }

    /**
     * Called after the S3Bolt rotates a file.
     */
    @Override
    public void reset() {
        lastRotation = System.currentTimeMillis();
    }

}
