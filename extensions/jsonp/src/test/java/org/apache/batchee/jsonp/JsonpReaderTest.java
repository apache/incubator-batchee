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
package org.apache.batchee.jsonp;

import org.apache.batchee.jsonp.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemWriter;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.json.JsonObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class JsonpReaderTest {
    @Test
    public void read() {
        IOs.write("target/work/jsonp-input.json", "[" +
            "  {" +
            "    \"v1\":\"record 1 # field 1\"," +
            "    \"v2\":\"record 1 # field 2\"" +
            "  }," +
            "  {" +
            "    \"v1\":\"record 2 # field 1\"," +
            "    \"v2\":\"record 2 # field 2\"" +
            "  }" +
            "]");

        final JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("jsonp-reader", new Properties()));
        assertEquals(Writer.ITEMS.size(), 2);
        for (int i = 1; i < Writer.ITEMS.size() + 1; i++) {
            final JsonObject record = Writer.ITEMS.get(i - 1);
            assertEquals("record " + i + " # field 1", record.getString("v1"));
            assertEquals("record " + i + " # field 2", record.getString("v2"));
        }
    }

    public static class Writer implements ItemWriter {
        public static List<JsonObject> ITEMS = new ArrayList<JsonObject>(2);

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
            ITEMS.addAll(List.class.cast(items));
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }
}
