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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import org.apache.batchee.extras.buffered.BufferedItemReader;
import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BufferedItemReaderTest {

    @Test
    public void testBufferedRead() throws Exception {
        DummyWriter.getItems().clear();

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("buffered-read", new Properties()));

        Assert.assertEquals(DummyWriter.getItems().size(), 3);
        Assert.assertEquals(DummyWriter.getItems().get(0), "A");
        Assert.assertEquals(DummyWriter.getItems().get(1), "B");
        Assert.assertEquals(DummyWriter.getItems().get(2), "C");
    }

    @Test
    public void testBufferedRead_iterableNull() throws Exception {
        DummyWriter.getItems().clear();

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("buffered-read-null", new Properties()));

        Assert.assertTrue(DummyWriter.getItems().isEmpty(), "items must be empty");
    }


    public static class MyBufferedItemReader extends BufferedItemReader<String> {
        @Override
        public Iterator<String> readAllItems() {
            return Arrays.asList("A", "B", "C").iterator();
        }
    }

    public static class MyNullBufferedItemReader extends BufferedItemReader<Number> {

        @Override
        public Iterator<Number> readAllItems() {
            return null;
        }
    }

    public static class DummyWriter extends AbstractItemWriter {
        private static List<Object> items = new ArrayList<Object>();

        @Override
        public void writeItems(List<Object> items) throws Exception {
            this.items.addAll(items);
        }

        public static List<Object> getItems() {
            return items;
        }
    }
}
