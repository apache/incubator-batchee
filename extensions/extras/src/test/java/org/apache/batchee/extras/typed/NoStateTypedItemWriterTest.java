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
package org.apache.batchee.extras.typed;

import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NoStateTypedItemWriterTest {

    @Test
    public void testDoWrite() throws Exception {
        MyNoStateTypedItemWriter.items.clear();

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("no-state-writer", new Properties()));

        Assert.assertFalse(MyNoStateTypedItemWriter.items.isEmpty(), "items must be not empty");
        Assert.assertEquals(MyNoStateTypedItemWriter.items.size(), 3);
        Assert.assertEquals(MyNoStateTypedItemWriter.items.get(0).longValue(), 12L);
        Assert.assertEquals(MyNoStateTypedItemWriter.items.get(1).longValue(), 45L);
        Assert.assertEquals(MyNoStateTypedItemWriter.items.get(2).longValue(), 113L);
    }


    public static class DummyItemReader extends AbstractItemReader {

        private static final long[] DUMMY_ITEMS_TO_READ = new long[]{12L, 45L, 113L};
        private int count;

        @Override
        public Object readItem() throws Exception {
            return count == DUMMY_ITEMS_TO_READ.length ? null : DUMMY_ITEMS_TO_READ[count++];
        }
    }

    public static class MyNoStateTypedItemWriter extends NoStateTypedItemWriter<Long> {

        public static List<Long> items = new ArrayList<Long>();

        @Override
        protected void doWriteItems(List<Long> items) {
            MyNoStateTypedItemWriter.items.addAll(items);
        }
    }
}
