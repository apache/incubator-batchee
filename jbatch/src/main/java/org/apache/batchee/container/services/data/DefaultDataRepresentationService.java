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
package org.apache.batchee.container.services.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

import org.apache.batchee.container.exception.BatchContainerServiceException;
import org.apache.batchee.container.util.TCCLObjectInputStream;
import org.apache.batchee.spi.DataRepresentationService;


/**
 * Default implementation of the {@link DataRepresentationService}
 */
public class DefaultDataRepresentationService implements DataRepresentationService {

    public static final String BATCHEE_SPLIT_TOKEN = ":";


    public static final String BATCHEE_DATA_PREFIX = "BatchEE_data" + BATCHEE_SPLIT_TOKEN;
    private Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    private static final Logger LOGGER = Logger.getLogger(DefaultDataRepresentationService.class.getName());

    @Override
    public void init(Properties batchConfig) {
        // nothing to do
    }

    @Override
    public <T> byte[] toInternalRepresentation(T dataObject) {
        if (dataObject == null) {
            return null;
        }

        byte[] serialValue = convertJavaNativeTypes(dataObject);
        if (serialValue == null) {
            serialValue = convertJava7DateTypes(dataObject);
        }
        if (serialValue == null) {
            // as last resort we do a simple java serialisation
            serialValue = convertSerializableObjectTypes(dataObject);
        }

        return serialValue;
    }

    @Override
    public <T> T toJavaRepresentation(byte[] internalRepresentation) {
        if (internalRepresentation == null) {
            return null;
        }

        T data = null;
        String stringRep = new String(internalRepresentation, UTF8_CHARSET);

        if (stringRep.startsWith(BATCHEE_DATA_PREFIX)) {
            String dataVal = stringRep.substring(BATCHEE_DATA_PREFIX.length());
            String typeVal = dataVal.substring(0, dataVal.indexOf(BATCHEE_SPLIT_TOKEN));
            String valueVal = dataVal.substring(dataVal.indexOf(BATCHEE_SPLIT_TOKEN) + BATCHEE_SPLIT_TOKEN.length());

            data = convertBackJavaNativeTypes(typeVal, valueVal);
            if (data == null) {
                data = convertBackJava7DateTypes(typeVal, valueVal);
            }
        }


        if (data == null) {
            data = convertBackSerializableObjectTypes(internalRepresentation);
        }

        // as last resort we do a simple java deserialisation


        if (data == null) {
            throw new IllegalStateException("Cannot convert back BatchEE data: " + internalRepresentation);
        }

        return data;
    }

    /**
     * This method converts java native types to a 'nice' string representation
     * which will be stored in the database.
     * @return the String representation or {@code null} if the dataObject was not a native Java type
     */
    private <T> byte[] convertJavaNativeTypes(T dataObject) {
        // convert int, Integer, etc
        if (dataObject instanceof Integer ||
            dataObject instanceof String ||
            dataObject instanceof Long ||
            dataObject instanceof Float ||
            dataObject instanceof Double) {
            return toBatchEeData(dataObject.getClass(), dataObject.toString());
        }

        //X TODO what else?
        return null;
    }

    private <T> T convertBackJavaNativeTypes(String typeVal, String value) {
        if (Integer.class.getName().equals(typeVal)) {
            return (T) Integer.valueOf(value);
        }
        if (Long.class.getName().equals(typeVal)) {
            return (T) Long.valueOf(value);
        }
        if (String.class.getName().equals(typeVal)) {
            return (T) value;
        }
        if (Float.class.getName().equals(typeVal)) {
            return (T) Float.valueOf(value);
        }
        if (Double.class.getName().equals(typeVal)) {
            return (T) Double.valueOf(value);
        }

        return null;
    }


    /**
     * Convert javas internal Date objects to db representation.
     *
     * The converted types are:
     * <ul>
     *     <li>java.util.Date</li>
     *     <li>java.sql.Date</li>
     *     <li>java.sql.Timestamp</li>
     * </ul>
     */
    private <T> byte[] convertJava7DateTypes(T dataObject) {
        if (dataObject.getClass().equals(Date.class)) {
            return toBatchEeData(Date.class, getSimpleDateFormat().format((Date) dataObject));
        }
        if (dataObject.getClass().equals(Timestamp.class)) {
            String val = String.format("%s.%09d",
                    getTimestampDateFormat().format((Timestamp) dataObject),
                    ((Timestamp) dataObject).getNanos());
            return toBatchEeData(Timestamp.class, val);
        }

        return null;
    }

    private <T> T convertBackJava7DateTypes(String typeVal, String valueVal) {
        if (Date.class.getName().equals(typeVal)) {
            try {
                return (T) getSimpleDateFormat().parse(valueVal);
            } catch (ParseException e) {
                LOGGER.warning("Could not parse Date - format must be yyyy-MM-dd'T'HH:mm:ss'Z' but value is " + valueVal);
            }
        }
        if (Timestamp.class.getName().equals(typeVal)) {
            try {
                int lastDot = valueVal.lastIndexOf('.');
                String datePart = valueVal.substring(0, lastDot);
                Date dt = getTimestampDateFormat().parse(datePart);
                Timestamp tst = new Timestamp(dt.getTime());
                String nanoPart = valueVal.substring(lastDot + 1);
                tst.setNanos(Integer.parseInt(nanoPart));
                return (T) tst;
            } catch (ParseException e) {
                LOGGER.warning("Could not parse Date - format must be yyyy-MM-dd'T'HH:mm:ss'Z' but value is " + valueVal);
            }
        }
        return null;
    }



    /**
     * This is the default operation if no other way to serialise the data was used
     */
    private <T> byte[] convertSerializableObjectTypes(T dataObject) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(dataObject);
            oos.close();
        } catch (IOException e) {
            throw new BatchContainerServiceException("Cannot convert data for [" + dataObject.toString() + "]");
        }
        return baos.toByteArray();
    }

    private <T> T convertBackSerializableObjectTypes(byte[] internalRepresentation) {
        final ByteArrayInputStream readerChkptBA = new ByteArrayInputStream(internalRepresentation);
        TCCLObjectInputStream readerOIS;
        try {
            // need to use the TCCL in case the batch is in a webapp but jbatch runtime is provided in a parent ClassLoader
            readerOIS = new TCCLObjectInputStream(readerChkptBA);
            T instance = (T) readerOIS.readObject();
            readerOIS.close();
            return instance;
        } catch (final Exception ex) {
            return null;
        }
    }

    private byte[] toBatchEeData(Class<?> type, String stringRepresentation) {
        return (BATCHEE_DATA_PREFIX + type.getName() + BATCHEE_SPLIT_TOKEN + stringRepresentation).getBytes(UTF8_CHARSET);
    }

    private SimpleDateFormat getSimpleDateFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    }

    private SimpleDateFormat getTimestampDateFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    }

}
