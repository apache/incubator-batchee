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
package org.apache.batchee.test.metric;

import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class PartitionMetricsTest {
    @Test
    public void run() {
        final JobOperator op = BatchRuntime.getJobOperator();
        final long id = op.start("partition-metrics", null);
        Batches.waitForEnd(op, id);
        final List<StepExecution> steps = op.getStepExecutions(id);
        assertEquals(1, steps.size());
        final StepExecution exec = steps.iterator().next();
        final Metric[] metrics = exec.getMetrics();
        int checked = 0;
        for (final Metric metric : metrics) {
            if (Metric.MetricType.ROLLBACK_COUNT == metric.getType()) {
                assertEquals(metric.getValue(), 1);
                checked++;
            } else if (Metric.MetricType.READ_SKIP_COUNT == metric.getType()) {
                assertEquals(metric.getValue(), 1);
                checked++;
            }
        }
        assertEquals(checked, 2);
    }

    public static class Reader extends AbstractItemReader {
        @Inject
        @BatchProperty
        private Integer idx;

        @Override
        public Object readItem() throws Exception {
            try {
                switch (idx) {
                    case 1:
                        throw new IllegalArgumentException();
                    case 2:
                        throw new FileNotFoundException();
                    default:
                }
            } finally {
                idx = 0;
            }
            return null;
        }
    }

    public static class Writer extends AbstractItemWriter {
        @Override
        public void writeItems(final List<Object> items) throws Exception {
            // no-op
        }
    }
}
