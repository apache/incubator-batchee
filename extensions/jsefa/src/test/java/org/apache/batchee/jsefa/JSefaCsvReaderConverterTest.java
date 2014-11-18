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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import org.apache.batchee.extras.typed.NoStateTypedItemWriter;
import org.apache.batchee.jsefa.bean.RecordWithConverter;
import org.apache.batchee.jsefa.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests if converter gets picked up if
 * specified in Object annotation
 */
public class JSefaCsvReaderConverterTest {

    @Test
    public void testRead() throws Exception {
        String path = "target/work/JSefaCsvReaderWithConverter.csv";

        IOs.write(path,
                  "string1;123;1;201007161200\n" +
                  "string2;345;2;199004041350\n" +
                  "string3;987654321;1;197905072358");

        Properties props = new Properties();
        props.setProperty("input", path);
        props.setProperty("specialRecordDelimiter", ";");

        JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("jsefa-csv-reader-converter", props));

        Set<RecordWithConverter> expectedItems = new HashSet<RecordWithConverter>();
        expectedItems.add(new RecordWithConverter("string1", 123L, RecordWithConverter.RecordEnum.ONE, new GregorianCalendar(2010, Calendar.JULY, 16, 12, 0).getTime()));
        expectedItems.add(new RecordWithConverter("string2", 345L, RecordWithConverter.RecordEnum.TWO, new GregorianCalendar(1990, Calendar.APRIL, 4, 13, 50).getTime()));
        expectedItems.add(new RecordWithConverter("string3", 987654321L, RecordWithConverter.RecordEnum.ONE, new GregorianCalendar(1979, Calendar.MAY, 7, 23, 58).getTime()));

        Assert.assertEquals(Storage.STORAGE.size(), expectedItems.size());
        Assert.assertTrue(Storage.STORAGE.containsAll(expectedItems));
    }


    public static class Storage extends NoStateTypedItemWriter<RecordWithConverter> {
        public static final Set<RecordWithConverter> STORAGE = new HashSet<RecordWithConverter>();


        @Override
        protected void doWriteItems(List<RecordWithConverter> items) {
            STORAGE.addAll(items);
        }
    }
}
