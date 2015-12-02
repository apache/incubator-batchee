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

import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.json.Json;
import java.io.Serializable;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class JsonpWriterTest {
    @Test
    public void write() {
        final JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("jsonp-writer", new Properties()));
        final String output = IOs.slurp("target/work/jsonp-output.json");
        assertEquals(output.replace("\n", "").replace("\r", "").replace(" ", "").replace("\t", ""),
                    "[{\"v1\":\"v11\",\"v2\":\"v21\"},{\"v1\":\"v12\",\"v2\":\"v22\"}]");
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
                return Json.createObjectBuilder().add("v1", "v1" + count).add("v2", "v2" + count).build();
            }
            return null;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }
}
