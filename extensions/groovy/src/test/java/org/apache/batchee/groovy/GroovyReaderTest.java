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
package org.apache.batchee.groovy;

import org.apache.batchee.groovy.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class GroovyReaderTest {
    @Test
    public void read() {
        IOs.write("target/work/reader.groovy", "package org.apache.batchee.groovy\n" +
            "\n" +
            "import javax.batch.api.chunk.ItemReader\n" +
            "\n" +
            "class GReader implements ItemReader {\n" +
            "    def count = 0\n" +
            "\n" +
            "    @Override\n" +
            "    void open(Serializable checkpoint) throws Exception {\n" +
            "\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    void close() throws Exception {\n" +
            "\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    Object readItem() throws Exception {\n" +
            "        if (count++ < 2) {\n" +
            "            \"groovy_\" + count\n" +
            "        } else {\n" +
            "            null\n" +
            "        }\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    Serializable checkpointInfo() throws Exception {\n" +
            "        return null\n" +
            "    }\n" +
            "}\n");
        final JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("groovy-reader", new Properties()));
        assertEquals(Writer.ITEMS.size(), 2);
        assertEquals("groovy_1", Writer.ITEMS.get(0));
        assertEquals("groovy_2", Writer.ITEMS.get(1));
    }

    public static class Writer implements ItemWriter {
        public static List<Object> ITEMS = new ArrayList<Object>(2);

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
}
