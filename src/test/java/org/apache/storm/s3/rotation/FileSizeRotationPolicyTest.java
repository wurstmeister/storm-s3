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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileSizeRotationPolicyTest {


    @Test
    public void testMark() throws Exception {
        FileSizeRotationPolicy policy = new FileSizeRotationPolicy(1.0F, FileSizeRotationPolicy.Units.KB);
        assertFalse(policy.mark(1000));
        assertFalse(policy.mark(23));
        assertTrue(policy.mark(1));
        policy.reset();
        assertFalse(policy.mark(0L));
    }

}
