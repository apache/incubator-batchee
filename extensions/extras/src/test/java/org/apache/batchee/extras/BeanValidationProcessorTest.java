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
package org.apache.batchee.extras;

import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class BeanValidationProcessorTest {
    @Test
    public void processOk() throws Exception {
        assertEquals(BatchStatus.COMPLETED, process("true"));
    }

    @Test
    public void processKo() throws Exception {
        assertEquals(BatchStatus.FAILED, process("false"));
    }

    private BatchStatus process(final String isOk) {
        final Properties jobParameters = new Properties();
        jobParameters.setProperty("ok", isOk);
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("bean-validation-processor", jobParameters);
        Batches.waitForEnd(jobOperator, id);
        return jobOperator.getJobExecution(id).getBatchStatus();
    }

    public static class TwoItemsReader implements ItemReader {
        @Inject
        @BatchProperty
        private String ok;

        private boolean done = false;

        @Override
        public void open(Serializable checkpoint) throws Exception {
            // no-op
        }

        @Override
        public void close() throws Exception {
            // no-op
        }

        @Override
        public Object readItem() throws Exception {
            if (done) {
                return null;
            }
            done = true;

            if ("true".equalsIgnoreCase(ok)) {
                return new Bean("not null is fine");
            }
            return new Bean(null);
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }

        public static class Bean {
            @NotNull
            private String name;

            public Bean(final String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }
        }
    }
}
