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
package org.apache.batchee.camel;

import org.apache.batchee.util.Batches;
import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class CamelProcessorTest {
    @Test
    public void process() throws Exception {
        final List<Exchange> exchanges = new ArrayList<Exchange>(2);
        final Consumer consumer = CamelBridge.CONTEXT.getEndpoint("direct:processor").createConsumer(new Processor() {
            @Override
            public void process(final Exchange exchange) throws Exception {
                exchanges.add(exchange);
            }
        });
        consumer.start();

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("camel-processor", new Properties()));
        assertEquals(StoreItems.ITEMS.size(), 2);
        assertEquals(exchanges.size(), 2);

        for (int i = 1; i <= 2; i++) {
            assertEquals("" + i, StoreItems.ITEMS.get(i - 1));
            assertEquals("" + i, exchanges.get(i - 1).getIn().getBody());
        }

        consumer.stop();
    }

    public static class StoreItems implements ItemWriter {
        public static final List<Object> ITEMS = new ArrayList<Object>(2);

        @Override
        public void open(final Serializable checkpoint) throws Exception {
            // no-op
        }

        @Override
        public void close() throws Exception {
            // no-op
        }

        @Override
        public void writeItems(final List<Object> items) throws Exception {
            ITEMS.addAll(items);
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }

    public static class TwoItemsReader implements ItemReader {
        private int count = 0;

        @Override
        public void open(final Serializable checkpoint) throws Exception {
            // no-op
        }

        @Override
        public void close() throws Exception {
            // no-op
        }

        @Override
        public Object readItem() throws Exception {
            if (count++ < 2) {
                return "" + count;
            }
            return null;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }
}
