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
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.direct.DirectEndpoint;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemProcessor;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class CamelReaderTest {
    @Test
    public void read() throws Exception {
        final ProducerTemplate tpl = CamelBridge.CONTEXT.createProducerTemplate();

        final JobOperator jobOperator = BatchRuntime.getJobOperator();

        final long id = jobOperator.start("camel-reader", new Properties());

        while (DirectEndpoint.class.cast(CamelBridge.CONTEXT.getEndpoint("direct:reader")).getConsumer() == null) {
            Thread.sleep(100);
        }

        tpl.sendBody("direct:reader", "input#1");
        tpl.sendBody("direct:reader", null);

        Batches.waitForEnd(jobOperator, id);

        assertEquals(StoreItems.ITEMS.size(), 1);
        assertEquals("input#1", StoreItems.ITEMS.get(0));
    }

    public static class StoreItems implements ItemProcessor {
        public static final List<Object> ITEMS = new ArrayList<Object>(2);

        @Override
        public Object processItem(final Object item) throws Exception {
            ITEMS.add(item);
            return item;
        }
    }
}
