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
package org.apache.batchee.test;

import org.testng.annotations.Test;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.StepExecution;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class JobLauncherTest {
    @Test
    public void start() {
        final JobExecution execution = JobLauncher.start("sleep", new Properties() {{
            setProperty("duration", "500");
        }});
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
    }

    @Test
    public void quickStart() {
        final JobExecution execution = new JobLauncher("sleep").start(new Properties() {{
            setProperty("duration", "500");
        }});
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
    }

    @Test
    public void steps() {
        final JobLauncher launcher = new JobLauncher("sleep");
        launcher.start(new Properties() {{
            setProperty("duration", "10");
        }});
        final List<StepExecution> executions = launcher.getLastStepExecutions();
        assertEquals(1, executions.size());
        assertEquals("OK", executions.iterator().next().getExitStatus());
    }
}
