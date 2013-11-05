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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class FlatFileItemReaderTest {
    @Test
    public void read() throws Exception {
        final String path = "target/work/FlatFileItemReader.txt";

        final Properties jobParams = new Properties();
        jobParams.setProperty("input", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        IOs.write(path, "line 1\r\n#ignored\r\nline2");
        Batches.waitForEnd(jobOperator, jobOperator.start("flat-file-reader", jobParams));
        assertEquals(StoreItems.ITEMS.size(), 2);
    }

    public static class StoreItems implements ItemProcessor {
        public static final Collection<Object> ITEMS = new ArrayList<Object>();

        @Override
        public Object processItem(final Object item) throws Exception {
            ITEMS.add(item);
            return item;
        }
    }
}
