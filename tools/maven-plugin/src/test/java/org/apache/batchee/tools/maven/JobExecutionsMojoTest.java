/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.tools.maven;

import org.testng.annotations.Test;

import java.util.Locale;

import static org.apache.batchee.tools.maven.BatchEEMojoTestFactory.mojo;
import static org.apache.batchee.util.Batches.waitForEnd;
import static org.testng.Assert.assertTrue;

public class JobExecutionsMojoTest {
    @Test
    public void executions() throws Exception {
        final JobExecutionsMojo mojo = mojo(JobExecutionsMojo.class);
        final long id = mojo.getOrCreateOperator().start("simple", null);
        waitForEnd(mojo.getOrCreateOperator(), id);

        mojo.instanceId = mojo.getOrCreateOperator().getJobInstance(id).getInstanceId();
        mojo.jobName = "simple";
        mojo.execute();

        final String output = BatchEEMojoTestFactory.output(mojo).toLowerCase(Locale.ENGLISH);
        assertTrue(output.contains("job executions for job instance #0 (simple):"));
        assertTrue(output.contains("id = #0, batch status = completed"));
    }
}
