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

import org.apache.batchee.jsefa.bean.Address;
import org.apache.batchee.jsefa.bean.Person;
import org.apache.batchee.jsefa.bean.PersonWithAddress;
import org.apache.batchee.jsefa.bean.Record;
import org.apache.batchee.jsefa.util.CsvUtil;
import org.apache.batchee.jsefa.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JSefaCsvWriterTest {
    @Test
    public void read() throws Exception {
        final String path = "target/work/JSefaCsvWriter.txt";

        final Properties jobParams = new Properties();
        jobParams.setProperty("output", path);

        final JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start("jsefa-csv-writer", jobParams));
        final String output = IOs.slurp(path);

        assertTrue(output.contains("v1_1;v1_2"));
        assertTrue(output.contains("v2_1;v2_2"));
    }

    @Test
    public void testWriteMultipleObjectTypes() {

        startBatchAndAssertResult("target/work/JSefaCsvWriterMulitObjectTypes.csv",
                                  "jsefa-csv-writer-multiOjectTypes",
                                  new Properties(),
                                  MultipleObjectTypesReader.STORAGE);
    }

    @Test
    public void testWriteNestedObjects() {

        startBatchAndAssertResult("target/work/JSefaCsvWriterNestedObjects.csv",
                                  "jsefa-csv-writer-nestedObjects",
                                  new Properties(),
                                  NestedObjectReader.STORAGE);
    }


    private void startBatchAndAssertResult(String path,
                                           String jobName,
                                           Properties jobProperties,
                                           List storage) {

        jobProperties.setProperty("output", path);

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start(jobName, jobProperties));

        List<String> batchOutput = IOs.getLines(path);
        assertEquals(batchOutput.size(), storage.size());

        for (int i = 0; i < batchOutput.size(); i++) {
            assertEquals(batchOutput.get(i), CsvUtil.toCsv(storage.get(i)));
        }
    }

    public static class TwoItemsReader implements ItemReader {
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
                return new Record("v" + count + "_");
            }
            return null;
        }

        @Override
        public Serializable checkpointInfo() throws Exception {
            return null;
        }
    }

    public static class MultipleObjectTypesReader extends AbstractItemReader {
        static final List<Object> STORAGE = new ArrayList<Object>();

        private int count;

        @Override
        public Object readItem() throws Exception {

            if (count++ == 10) {
                return null;
            }

            Object item;
            if (count % 2 == 0) {
                item =  new Person("firstName_" + count, "lastName_" + count);
            }
            else {
                item = new Address("street_" + count, "zip_" + count, "city_" + count);
            }

            STORAGE.add(item);
            return item;
        }
    }

    public static class NestedObjectReader extends AbstractItemReader {
        static final List<Person> STORAGE = new ArrayList<Person>(10);

        private int count;

        @Override
        public Object readItem() throws Exception {

            if (count++ == 10) {
                return null;
            }

            PersonWithAddress item = new PersonWithAddress("firstName_" + count,
                                                           "lastName_" + count,
                                                           new Address("street_" + count,
                                                                     "zip_" + count,
                                                                     "city_" + count));
            STORAGE.add(item);

            return item;
        }
    }
}
