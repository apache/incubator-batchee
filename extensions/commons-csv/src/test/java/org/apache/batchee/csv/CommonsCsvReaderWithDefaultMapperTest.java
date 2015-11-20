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
package org.apache.batchee.csv;

import org.apache.batchee.csv.mapper.Csv;
import org.apache.batchee.csv.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemProcessor;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class CommonsCsvReaderWithDefaultMapperTest {
    @BeforeMethod
    public void clean() {
        StoreItems.ITEMS.clear();
    }

    @Test
    public void read() throws Exception {
        final String path = "target/work/CommonsCsvReaderWithDefaultMapperTestread.txt";

        final Properties jobParams = new Properties();
        jobParams.setProperty("input", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        IOs.write(path, "v11,v12\nv21,v22\nv31,v32");
        Batches.waitForEnd(jobOperator, jobOperator.start("csv-reader-defaultmapper", jobParams));

        final int size = StoreItems.ITEMS.size();
        assertEquals(size, 3);
        for (int i = 1; i <= size; i++) {
            final Indexed record = Indexed.class.cast(StoreItems.ITEMS.get(i - 1));
            assertEquals("v" + i + "1", record.c1);
            assertEquals("v" + i + "2", record.c2);
        }
    }

    @Test
    public void header() throws Exception {
        final String path = "target/work/CommonsCsvReaderWithDefaultMapperTestheader.txt";

        final Properties jobParams = new Properties();
        jobParams.setProperty("input", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        IOs.write(path, "c1,c2,int\nv11,v12\nv21,v22,1\nv31,v32,2");
        Batches.waitForEnd(jobOperator, jobOperator.start("csv-reader-defaultmappername", jobParams));

        final int size = StoreItems.ITEMS.size();
        assertEquals(size, 3);
        for (int i = 1; i <= size; i++) {
            final Named record = Named.class.cast(StoreItems.ITEMS.get(i - 1));
            assertEquals("v" + i + "1", record.c1);
            assertEquals("v" + i + "2", record.c2);
        }
    }

    public static class StoreItems implements ItemProcessor {
        public static final List<Object> ITEMS = new ArrayList<Object>(3);

        @Override
        public Object processItem(final Object item) throws Exception {
            ITEMS.add(item);
            return item;
        }
    }

    public static class Indexed {
        @Csv(index = 0)
        private String c1;

        @Csv(index = 1)
        private String c2;
    }

    public static class Named {
        @Csv(name = "c1")
        private String c1;

        @Csv(name = "c2")
        private String c2;
    }
}
