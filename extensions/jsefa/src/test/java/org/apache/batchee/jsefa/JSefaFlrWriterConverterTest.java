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

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;

import org.apache.batchee.extras.typed.NoStateTypedItemReader;
import org.apache.batchee.jsefa.bean.RecordWithConverter;
import org.apache.batchee.jsefa.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests if converter gets picked up if
 * specified in Object annotation
 */
public class JSefaFlrWriterConverterTest {

    @Test
    public void testWrite() throws Exception {
        String path = "target/work/JSefaFlrWriterWithConverter.txt";

        Properties props = new Properties();
        props.setProperty("output", path);

        JobOperator operator = BatchRuntime.getJobOperator();
        Batches.waitForEnd(operator, operator.start("jsefa-flr-writer-converter", props));

        Assert.assertEquals(IOs.slurp(path), toFlrString());
    }


    private String toFlrString() {
        StringBuilder builder = new StringBuilder();

        for (RecordWithConverter record : RecordReader.STORAGE) {
            builder.append(rightPad(record.getStringValue(), 10))
                   .append(rightPad(record.getLongValue().toString(), 9))
                   .append(rightPad(record.getEnumValue().getCode(), 3))
                   .append(new SimpleDateFormat("yyyyMMddHHmm").format(record.getDateValue())).append(System.getProperty("line.separator"));
        }

        return builder.toString();
    }

    private String rightPad(String value, int length) {
        char[] pad = new char[length - value.length()];
        for (int i = 0; i < pad.length; i++) {
            pad[i] = ' ';
        }

        return value.concat(new String(pad));
    }


    public static class RecordReader extends NoStateTypedItemReader<RecordWithConverter> {
        public static final List<RecordWithConverter> STORAGE = new LinkedList<RecordWithConverter>();

        private int count;


        @Override
        protected RecordWithConverter doRead() {
            if (count++ == 5) {
                return null;
            }

            RecordWithConverter record = new RecordWithConverter("string" + count,
                                                                 count * 100000L,
                                                                 RecordWithConverter.RecordEnum.values()[new Random().nextInt(2)],
                                                                 new GregorianCalendar(count + 2000, count, count + 5, count + 12, count + 30).getTime());

            STORAGE.add(record);

            return record;
        }
    }
}
