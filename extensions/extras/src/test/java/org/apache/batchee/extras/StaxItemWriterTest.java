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

import org.apache.batchee.util.Batches;
import org.apache.batchee.extras.util.IOs;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class StaxItemWriterTest {
    @Test
    public void write() throws Exception {
        final String path = "target/work/StaxItemWriter.xml";

        final Properties jobParams = new Properties();
        jobParams.setProperty("output", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("stax-writer", jobParams));
        final String content = IOs.slurp(path);
        assertEquals(content.replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", ""), "<root><foo><value>item 1</value></foo><foo><value>item 2</value></foo></root>");
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
                return new Foo("item " + count);
            }
            return null;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }

    @XmlRootElement
    public static class Foo {
        private String value;

        public Foo() {
            // no-op
        }

        public Foo(final String s) {
            value = s;
        }

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
