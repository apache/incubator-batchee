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
package org.apache.batchee.jsefa.bean;

import net.sf.jsefa.csv.annotation.CsvDataType;
import net.sf.jsefa.csv.annotation.CsvField;
import org.apache.batchee.jsefa.Header;

@CsvDataType
public class RecordWithHeader {

    @Header("VALUE_HEADER")
    @CsvField(pos = 1)
    private String value;

    @Header("ANOTHER_VALUE_HEADER")
    @CsvField(pos = 2)
    private String anotherValue;


    private RecordWithHeader() {
    }

    public RecordWithHeader(int count) {
        this.value = "value_" + count;
        this.anotherValue = "anotherValue_" + count;
    }


    public String getValue() {
        return value;
    }

    public String getAnotherValue() {
        return anotherValue;
    }
}
