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

import org.apache.batchee.extras.typed.NoStateTypedItemReader;
import org.apache.batchee.jsefa.bean.Address;
import org.apache.batchee.jsefa.bean.Person;
import org.apache.batchee.jsefa.bean.PersonWithAddress;
import org.apache.batchee.jsefa.bean.Record;
import org.apache.batchee.jsefa.bean.RecordWithHeader;
import org.apache.batchee.jsefa.util.CsvUtil;
import org.apache.batchee.jsefa.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JSefaCsvWriterHeaderTest {


    /**
     * Tests a specified header via xml-file
     *
     * {@code <property name="header" value="VALUE1;VALUE2" />}
     */
    @Test
    public void testWriteWithPropertyHeader() {

        startBatchAndAssertResult("target/work/JSefaCsvWriterHeaderTest.csv",
                                  "jsefa-csv-writer-header",
                                  "VALUE1;VALUE2",
                                  RecordReader.STORAGE);
    }

    @Test
    public void testWriteHeaderFromFieldsOneType() {

        startBatchAndAssertResult("target/work/JSefaCsvWriterHeaderFromFieldsOnTypeTest.csv",
                                  "jsefa-csv-writer-header-fromFieldsOneType",
                                  "firstName;lastName",
                                  SimpleOneTypeReader.STORAGE);
    }

    @Test
    public void testWriteHeaderFromFieldsWithInheritance() {

        startBatchAndAssertResult("target/work/JSefaCsvWriterHeaderWithInheritance.csv",
                                  "jsefa-csv-writer-header-fromFieldsWithInheritance",
                                  "firstName;lastName;street;zip;city",
                                  InheritanceReader.STORAGE);
    }

    @Test
    public void testWriteHeaderFromFieldsMoreObjectTypes() {

        startBatchAndAssertResult("target/work/JSefaCsvWriterHeaderMoreObjects.csv",
                                  "jsefa-csv-writer-header-fromFieldsMoreObjects",
                                  "firstName;lastName;street;zip;city",
                                  MoreObjectReader.STORAGE);
    }

    @Test
    public void testWriteHeaderFromHeaderAnnotation() {

        startBatchAndAssertResult("target/work/JsefaCsvWriterHeaderFromAnnotation.csv",
                                  "jsefa-csv-writer-header-fromHeaderAnnotation",
                                  "VALUE_HEADER;ANOTHER_VALUE_HEADER",
                                  HeaderAnnotationReader.STORAGE);
    }


    private void startBatchAndAssertResult(String path,
                                           String jobName,
                                           String header,
                                           List storage) {

        Properties properties = new Properties();
        properties.setProperty("output", path);

        JobOperator jobOperator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(jobOperator, jobOperator.start(jobName, properties));

        List<String> lines = IOs.getLines(path);

        int expectedSize = header == null ? storage.size() : storage.size() + 1;
        Assert.assertEquals(lines.size(), expectedSize);

        for (int i = 0; i < lines.size(); i++) {

            String line = lines.get(i);
            String expected;

            if (header == null) {
                expected = CsvUtil.toCsv(storage.get(i));
            }
            else if (i == 0) {
                expected = header;
            }
            else {
                expected = CsvUtil.toCsv(storage.get(i - 1));
            }

            Assert.assertEquals(line, expected);
        }
    }


    public static class RecordReader extends NoStateTypedItemReader<Record> {

        static final List<Record> STORAGE = new ArrayList<Record>(5);


        private int count;

        @Override
        protected Record doRead() {

            if (count++ < 5) {
                Record record = new Record("Entry_" + count + "_Value_");

                STORAGE.add(record);
                return record;
            }

            return null;
        }
    }

    public static class SimpleOneTypeReader extends AbstractItemReader {

        static final List<Person> STORAGE = new ArrayList<Person>();


        private int count;


        @Override
        public Object readItem() throws Exception {

            if (count++ < 10) {

                Person person = new Person("firstName_" + count, "lastName_" + count);

                STORAGE.add(person);
                return person;
            }

            return null;
        }
    }

    public static class InheritanceReader extends AbstractItemReader {

        static final List<PersonWithAddress> STORAGE = new ArrayList<PersonWithAddress>();


        private int count;


        @Override
        public Object readItem() throws Exception {

            if (count++ < 10) {

                PersonWithAddress person = new PersonWithAddress("firstName_" + count,
                                                                 "lastName_" + count,
                                                                 new Address("street_" + count,
                                                                             "zip_" + count,
                                                                             "city_" + count));

                STORAGE.add(person);
                return person;
            }

            return null;
        }
    }

    public static class MoreObjectReader extends AbstractItemReader {

        static final List<Object> STORAGE = new ArrayList<Object>();


        private int count;


        @Override
        public Object readItem() throws Exception {

            Object item;
            if (count++ == 10) {
                item = null;
            }
            else if (count % 2 == 0) {
                item = new Person("firstName_" + count, "lastName_" + count);
            }
            else {
                item = new Address("street_" + count, "zip_" + count, "city_" + count);
            }

            if (item != null) {
                STORAGE.add(item);
            }

            return item;
        }
    }

    public static class HeaderAnnotationReader extends AbstractItemReader {

        static final List<RecordWithHeader> STORAGE = new ArrayList<RecordWithHeader>(5);


        private int count;

        @Override
        public Object readItem() throws Exception {

            if (count++ < 5) {

                RecordWithHeader record = new RecordWithHeader(count);
                STORAGE.add(record);
                return record;
            }

            return null;
        }
    }
}
