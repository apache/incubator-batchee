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

import javax.batch.api.chunk.ItemProcessor;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class StaxItemReaderTest {
    @Test
    public void read() throws Exception {
        final String path = "target/work/StaxItemReader.xml";

        final Properties jobParams = new Properties();
        jobParams.setProperty("tag", "bar");
        jobParams.setProperty("input", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        IOs.write(path, "<foo><bar><value>1</value></bar><bar><value>2</value></bar></foo>");
        Batches.waitForEnd(jobOperator, jobOperator.start("stax-reader", jobParams));
        assertEquals(StoreItems.ITEMS.size(), 2);
        assertEquals("1", StoreItems.ITEMS.get(0).getValue());
        assertEquals("2", StoreItems.ITEMS.get(1).getValue());
    }

    public static class StoreItems implements ItemProcessor {
        public static final List<Bar> ITEMS = new ArrayList<Bar>(2);

        @Override
        public Object processItem(final Object item) throws Exception {
            ITEMS.add(Bar.class.cast(item));
            return item;
        }
    }

    @XmlRootElement
    public static class Bar {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }
}
