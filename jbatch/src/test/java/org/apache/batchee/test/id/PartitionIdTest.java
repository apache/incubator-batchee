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
package org.apache.batchee.test.id;

import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.testng.Assert.assertEquals;

public class PartitionIdTest {
    @Test
    public void run() {
        final JobOperator op = BatchRuntime.getJobOperator();
        final long id = op.start("partition-execId", null);
        Batches.waitForEnd(op, id);

        final long stepId = op.getStepExecutions(id).iterator().next().getStepExecutionId();

        assertEquals(2, Reader.jobIds.size());
        assertEquals(2, Reader.stepIds.size());
        assertEquals(id, Reader.jobIds.get(1).longValue());
        assertEquals(Reader.jobIds.get(0), Reader.jobIds.get(1));
        assertEquals(stepId, Reader.stepIds.get(0).longValue());
        assertEquals(Reader.stepIds.get(0), Reader.stepIds.get(1));
    }

    public static class Reader extends AbstractItemReader {
        private static final List<Long> jobIds = new CopyOnWriteArrayList<Long>();
        private static final List<Long> stepIds = new CopyOnWriteArrayList<Long>();

        @Inject
        @BatchProperty
        private Integer idx;

        @Inject
        private StepContext stepContext;

        @Inject
        private JobContext jobContext;

        @Override
        public Object readItem() throws Exception {
            if (idx != null) {
                jobIds.add(jobContext.getExecutionId());
                stepIds.add(stepContext.getStepExecutionId());
            }
            try {
                return idx;
            } finally {
                idx = null;
            }
        }
    }

    public static class Writer extends AbstractItemWriter {
        @Override
        public void writeItems(final List<Object> items) throws Exception {
            // no-op
        }
    }
}
