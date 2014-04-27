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

import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Test for the abstract {@link NoStateTypedItemReader}
 */
public class NoStateTypedItemReaderTest {


    @Test
    public void testDoRead() throws Exception {
        DummyItemWriter.items.clear();

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("no-state-reader", new Properties()));

        Assert.assertFalse(DummyItemWriter.items.isEmpty(), "items must be not empty");
        Assert.assertEquals(DummyItemWriter.items.size(), 4);
        Assert.assertEquals(DummyItemWriter.items.get(0), "A");
        Assert.assertEquals(DummyItemWriter.items.get(1), "B");
        Assert.assertEquals(DummyItemWriter.items.get(2), "3");
        Assert.assertEquals(DummyItemWriter.items.get(3), "4");
    }


    public static class MyNoStateTypedItemReader extends NoStateTypedItemReader<String> {

        private static final String[] ITEMS_TO_READ = new String[] {"A","B", "3", "4"};
        private int count;

        @Override
        protected String doRead() {
            return count == ITEMS_TO_READ.length ? null : ITEMS_TO_READ[count++];
        }
    }

    public static class DummyItemWriter extends AbstractItemWriter {

        public static List<Object> items = new ArrayList<Object>();

        @Override
        public void writeItems(List<Object> items) throws Exception {
            DummyItemWriter.items.addAll(items);
        }
    }

}