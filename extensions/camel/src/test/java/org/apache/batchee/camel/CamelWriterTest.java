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
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.component.direct.DirectEndpoint;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

public class CamelWriterTest {
    @Test
    public void write() throws Exception {
        final ConsumerTemplate tpl = CamelBridge.CONTEXT.createConsumerTemplate();
        final Collection<Object> received = new ArrayList<Object>(2);

        final ExecutorService thread = Executors.newSingleThreadExecutor();
        thread.submit(new Runnable() {
            @Override
            public void run() {
                do {
                    received.add(tpl.receiveBody("direct:writer"));
                } while (received.size() < 2);
            }
        });
        thread.shutdown();

        do { // starting the listener in another thread w can get timing issues so ensuring we are in the right state
            Thread.sleep(20);
        } while (DirectEndpoint.class.cast(CamelBridge.CONTEXT.getEndpoint("direct:writer")).getConsumer() == null);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("camel-writer", new Properties()));

        thread.awaitTermination(5, TimeUnit.MINUTES); // wait end of the thread before checking received

        assertTrue(received.contains("1"), received.toString());
        assertTrue(received.contains("2"), received.toString());
    }

    public static class Reader implements ItemReader {
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
