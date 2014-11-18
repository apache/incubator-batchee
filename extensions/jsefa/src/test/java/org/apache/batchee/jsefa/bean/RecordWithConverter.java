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

import java.util.Date;

import org.apache.batchee.jsefa.converter.CustomDateConverter;
import org.apache.batchee.jsefa.converter.RecordEnumConverter;
import net.sf.jsefa.csv.annotation.CsvDataType;
import net.sf.jsefa.csv.annotation.CsvField;
import net.sf.jsefa.flr.annotation.FlrDataType;
import net.sf.jsefa.flr.annotation.FlrField;

@CsvDataType
@FlrDataType
public class RecordWithConverter {


    public enum RecordEnum {
        ONE("1"),
        TWO("2");

        private String code;

        private RecordEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }


        public static RecordEnum getByCode(String code) {
            for (RecordEnum recordEnum : values()) {
                if (recordEnum.code.equals(code)) {
                    return recordEnum;
                }
            }

            return null;
        }
    }


    @CsvField(pos = 1)
    @FlrField(pos = 1, length = 10)
    private String stringValue;

    @CsvField(pos = 2)
    @FlrField(pos = 2, length = 9)
    private Long longValue;

    @CsvField(pos = 3, converterType = RecordEnumConverter.class)
    @FlrField(pos = 3, length = 3, converterType = RecordEnumConverter.class)
    private RecordEnum enumValue;

    @CsvField(pos = 4, converterType = CustomDateConverter.class)
    @FlrField(pos = 4, length = 12, converterType = CustomDateConverter.class)
    private Date dateValue;


    public RecordWithConverter() {
        // no-op
    }

    public RecordWithConverter(String stringValue, Long longValue, RecordEnum enumValue, Date dateValue) {
        this.stringValue = stringValue;
        this.longValue = longValue;
        this.enumValue = enumValue;
        this.dateValue = dateValue;
    }


    public String getStringValue() {
        return stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public Long getLongValue() {
        return longValue;
    }

    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    public RecordEnum getEnumValue() {
        return enumValue;
    }

    public void setEnumValue(RecordEnum enumValue) {
        this.enumValue = enumValue;
    }

    public Date getDateValue() {
        return dateValue;
    }

    public void setDateValue(Date dateValue) {
        this.dateValue = dateValue;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        RecordWithConverter that = (RecordWithConverter) o;

        if (dateValue != null ? !dateValue.equals(that.dateValue) : that.dateValue != null) { return false; }
        if (enumValue != that.enumValue) { return false; }
        if (longValue != null ? !longValue.equals(that.longValue) : that.longValue != null) { return false; }
        if (stringValue != null ? !stringValue.equals(that.stringValue) : that.stringValue != null) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = stringValue != null ? stringValue.hashCode() : 0;
        result = 31 * result + (longValue != null ? longValue.hashCode() : 0);
        result = 31 * result + (enumValue != null ? enumValue.hashCode() : 0);
        result = 31 * result + (dateValue != null ? dateValue.hashCode() : 0);
        return result;
    }
}
