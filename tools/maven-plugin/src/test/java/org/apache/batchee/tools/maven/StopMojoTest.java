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

import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import java.util.Locale;
import java.util.Properties;

import static org.apache.batchee.tools.maven.BatchEEMojoTestFactory.mojo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class StopMojoTest {
    @Test
    public void stop() throws Exception {
        final StopMojo mojo = mojo(StopMojo.class);

        final long id = mojo.getOrCreateOperator().start("simple", new Properties() {{ setProperty("sleep", "5000"); }});
        JobExecution jobExecution;
        do {
            jobExecution = mojo.getOrCreateOperator().getJobExecution(id);
        } while (!BatchStatus.STARTED.equals(jobExecution.getBatchStatus()));

        mojo.executionId = id;
        mojo.execute();

        Batches.waitForEnd(id);

        final String output = BatchEEMojoTestFactory.output(mojo);
        assertEquals(BatchStatus.STOPPED, mojo.getOrCreateOperator().getJobExecution(Long.parseLong(output.substring(output.lastIndexOf("#") + 1).trim())).getBatchStatus());
        assertTrue(output.toLowerCase(Locale.ENGLISH).contains("stopped"));
    }
}
