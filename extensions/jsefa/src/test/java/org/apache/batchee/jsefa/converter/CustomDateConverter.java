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
package org.apache.batchee.jsefa.converter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.jsefa.common.converter.SimpleTypeConverter;


public class CustomDateConverter implements SimpleTypeConverter {

    @Override
    public String toString(Object o) {
        if (o == null || !(o instanceof Date)) {
            return null;
        }

        return getFormat().format((Date) o);
    }

    @Override
    public Date fromString(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }

        try {
            return getFormat().parse(s);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


    private SimpleDateFormat getFormat() {
        return new SimpleDateFormat("yyyyMMddHHmm");
    }
}
