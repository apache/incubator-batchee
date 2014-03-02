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

import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import java.util.List;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.testng.Assert.assertEquals;

public class AsyncProcessorTest {
    @Test
    public void async() throws Exception {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        final long id = jobOperator.start("async-processor", null);
        Batches.waitForEnd(jobOperator, id);
        assertEquals(BatchStatus.COMPLETED, jobOperator.getJobExecution(id).getBatchStatus());
    }



    public static class TwoItemsReader extends AbstractItemReader {
        private int count = 0;

        @Override
        public Object readItem() throws Exception {
            if (count++ < 2) {
                return "line " + count;
            }
            return null;
        }
    }

    public static class Delegate implements ItemProcessor {
        @Override
        public Object processItem(final Object o) throws Exception {
            return "line";
        }
    }

    public static class Writer extends AbstractItemWriter {
        @Override
        public void writeItems(final List<Object> objects) throws Exception {
            for (final Object o : objects) {
                assertThat(o, instanceOf(Future.class));
                assertEquals("line", Future.class.cast(o).get().toString());
            }
        }
    }
}
