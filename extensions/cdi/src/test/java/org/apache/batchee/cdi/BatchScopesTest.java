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
package org.apache.batchee.cdi;

import org.apache.batchee.cdi.component.Holder;
import org.apache.batchee.cdi.component.JobScopedBean;
import org.apache.batchee.cdi.component.StepScopedBean;
import org.apache.batchee.cdi.partitioned.PartitionedJobScopedReader;
import org.apache.batchee.cdi.testng.CdiContainerLifecycle;
import org.apache.batchee.util.Batches;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;

import java.util.Properties;

import static org.testng.Assert.*;

@Listeners(CdiContainerLifecycle.class)
public class BatchScopesTest {

    @BeforeMethod
    public void resetJobScopedBean() {
        JobScopedBean.reset();
    }

    @Test
    public void test() {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("cdi", null));

        assertEquals(2, Holder.JOB_SCOPED_IDS.size());
        assertEquals(2, Holder.STEP_SCOPED_IDS.size());

        assertEquals(Holder.JOB_SCOPED_IDS.get(0), Holder.JOB_SCOPED_IDS.get(1));
        assertNotSame(Holder.STEP_SCOPED_IDS.get(0), Holder.STEP_SCOPED_IDS.get(1));

        assertTrue(JobScopedBean.isDestroyed());
        assertTrue(StepScopedBean.isDestroyed());
    }

    @Test
    public void testPartitionedJobScoped() throws Exception {

        JobOperator jobOperator = BatchRuntime.getJobOperator();

        long executionId = jobOperator.start("partitioned-job-scoped", new Properties());

        Thread.sleep(100);

        assertEquals(PartitionedJobScopedReader.currentBeanId(), PartitionedJobScopedReader.originalBeanId());

        PartitionedJobScopedReader.stop();

        Thread.sleep(100);

        assertEquals(PartitionedJobScopedReader.currentBeanId(), PartitionedJobScopedReader.originalBeanId());
        assertFalse(JobScopedBean.isDestroyed(), "JobScopedBean must not be destroyed -> partition 2 is still running :(");

        PartitionedJobScopedReader.stopPartition2();

        assertEquals(Batches.waitFor(executionId), BatchStatus.COMPLETED);

        assertEquals(PartitionedJobScopedReader.currentBeanId(), PartitionedJobScopedReader.originalBeanId());
        assertTrue(JobScopedBean.isDestroyed());
    }

    @Test
    public void testPartitionedStepScoped() {

        JobOperator jobOperator = BatchRuntime.getJobOperator();

        BatchStatus status = Batches.waitFor(jobOperator.start("partitioned-step-scoped", new Properties()));
        assertEquals(status, BatchStatus.COMPLETED);
    }
}
