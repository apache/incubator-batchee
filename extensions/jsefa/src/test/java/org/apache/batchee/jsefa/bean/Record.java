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
import net.sf.jsefa.flr.annotation.FlrDataType;
import net.sf.jsefa.flr.annotation.FlrField;
import net.sf.jsefa.xml.annotation.XmlDataType;
import net.sf.jsefa.xml.annotation.XmlElement;

@CsvDataType
@FlrDataType
@XmlDataType(name = "record")
public class Record {
    @CsvField(pos = 1)
    @XmlElement(pos = 1)
    @FlrField(pos = 1, length = 3)
    private String value1;

    @CsvField(pos = 2)
    @XmlElement(pos = 2)
    @FlrField(pos = 2, length = 3)
    private String value2;

    public Record(final String s) {
        value1 = s + "1";
        value2 = s + "2";
    }

    public Record() {
        // no-op
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(final String value) {
        this.value1 = value;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(final String value) {
        this.value2 = value;
    }
}
