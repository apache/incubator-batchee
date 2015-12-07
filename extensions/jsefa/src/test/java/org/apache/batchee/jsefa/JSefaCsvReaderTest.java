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
package org.apache.batchee.jsefa;

import org.apache.batchee.extras.typed.TypedItemProcessor;
import org.apache.batchee.jsefa.bean.Address;
import org.apache.batchee.jsefa.bean.PersonWithAddress;
import org.apache.batchee.jsefa.bean.Record;
import org.apache.batchee.jsefa.util.CsvUtil;
import org.apache.batchee.jsefa.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.batch.api.chunk.ItemProcessor;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class JSefaCsvReaderTest {
    @Test
    public void read() throws Exception {
        final String path = "target/work/JSefaCsvReader.txt";

        final Properties jobParams = new Properties();
        jobParams.setProperty("input", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        IOs.write(path, "v11;v12\nv21;v22\nv31;v32");
        Batches.waitForEnd(jobOperator, jobOperator.start("jsefa-csv-reader", jobParams));

        final int size = StoreItems.ITEMS.size();
        assertEquals(size, 3);
        for (int i = 1; i <= size; i++) {
            final Record record = StoreItems.ITEMS.get(i - 1);
            assertEquals(record.getValue1(), "v" + i + "1");
            assertEquals(record.getValue2(), "v" + i + "2");
        }
    }


    @Test
    public void testReadWithHeader() {
        String path = "target/work/JsefaCsvReaderWithHeader.csv";

        Properties properties = new Properties();
        properties.setProperty("input", path);

        StringBuilder csvBuilder = new StringBuilder(200);
        csvBuilder.append("firstName;lastName;street;zip;city");
        for (int i = 0; i < 10; i++) {
            csvBuilder.append(IOs.LINE_SEPARATOR)
                      .append(CsvUtil.toCsv(new PersonWithAddress("firstName_" + i,
                                                                  "lastName_" + i,
                                                                  new Address("street_" + i,
                                                                              "zip_" + i,
                                                                              "city_" + i))));
        }

        IOs.write(path, csvBuilder.toString());

        JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("jsefa-csv-reader-header", properties));

        Assert.assertEquals(Storage.ITEMS.size(), 10);

    }

    public static class StoreItems implements ItemProcessor {
        public static final List<Record> ITEMS = new ArrayList<Record>(3);

        @Override
        public Object processItem(final Object item) throws Exception {
            ITEMS.add(Record.class.cast(item));
            return item;
        }
    }

    public static class Storage extends TypedItemProcessor<PersonWithAddress, PersonWithAddress> {

        static final List<PersonWithAddress> ITEMS = new ArrayList<PersonWithAddress>(10);

        @Override
        protected PersonWithAddress doProcessItem(PersonWithAddress item) {
            ITEMS.add(item);
            return item;
        }
    }
}
