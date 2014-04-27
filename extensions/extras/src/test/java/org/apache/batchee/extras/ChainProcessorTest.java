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

import org.apache.batchee.extras.typed.TypedItemProcessor;
import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ChainProcessorTest {
    @Test
    public void chain() throws Exception {
        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("chain-processor", new Properties()));

        assertEquals(P1.instance.items.size(), 2);
        assertEquals(P2.instance.items.size(), 2);

        final int p1Hash = P1.instance.hashCode();
        final int p2Hash = P2.instance.hashCode();

        assertTrue(P1.instance.items.contains("1 " + p1Hash));
        assertTrue(P1.instance.items.contains("2 " + p1Hash));
        assertTrue(P2.instance.items.contains("1 " + p1Hash + " " + p2Hash));
        assertTrue(P2.instance.items.contains("2 " + p1Hash + " " + p2Hash));
    }

    public static class P1 extends StoreItems {
        public static P1 instance;

        public P1() {
            instance = this;
        }
    }

    public static class P2 extends StoreItems {
        public static P2 instance;

        public P2() {
            instance = this;
        }
    }

    public static class TwoItemsReader implements ItemReader {
        private int count = 0;

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

    public static abstract class StoreItems extends TypedItemProcessor<String, String> {
        public final Collection<String> items = new ArrayList<String>();

        @Override
        protected String doProcessItem(String item) {
            final String value = "" + item + " " + hashCode();
            items.add(value);
            return value;
        }
    }
}
