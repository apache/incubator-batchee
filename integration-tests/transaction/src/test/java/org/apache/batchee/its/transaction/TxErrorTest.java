/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.its.transaction;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;

import java.util.List;

import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TxErrorTest {


    @Test
    public void testRolledBackDuringWork() {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        long executionId = jobOperator.start("txtest1", null);
        BatchStatus batchStatus = Batches.waitFor(jobOperator, executionId);
        Assert.assertEquals(batchStatus, BatchStatus.FAILED);
        Assert.assertEquals(TxErrorWriter1.written.intValue(), 3);

        List<StepExecution> stepExecutions = jobOperator.getStepExecutions(executionId);
        Assert.assertEquals(stepExecutions.size(), 1);
        StepExecution stepExecution = stepExecutions.get(0);
        Metric[] metrics = stepExecution.getMetrics();
        assertMetric(Metric.MetricType.READ_COUNT, 2, metrics);
        assertMetric(Metric.MetricType.WRITE_COUNT, 2, metrics);
        assertMetric(Metric.MetricType.ROLLBACK_COUNT, 1, metrics);
    }

    private void assertMetric(Metric.MetricType metricType, long expected, Metric[] metrics) {
        for (Metric metric : metrics) {
            if (metricType.equals(metric.getType())) {
                Assert.assertEquals(metric.getValue(), expected);
                return;
            }
        }
        Assert.fail("MetricType " + metricType + " not in collected metrics");
    }
}
