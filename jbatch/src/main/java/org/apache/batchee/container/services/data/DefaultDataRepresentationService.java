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
import java.lang.reflect.Method;
import java.nio.charset.Charset;
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

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private static final Logger LOGGER = Logger.getLogger(DefaultDataRepresentationService.class.getName());


    @Override
    public void init(Properties batchConfig) {
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
            serialValue = convertJava8DateTypes(dataObject);
        }
        if (serialValue == null) {
            serialValue = convertJodaDateTypes(dataObject);
        }
        if (serialValue == null) {
            serialValue = convertCustomEnumTypes(dataObject);
        }
        if (serialValue == null) {
            serialValue = convertCustomTypes(dataObject);
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
            if (data == null) {
                data = convertBackJava8DateTypes(typeVal, valueVal);
            }
            if (data == null) {
                data = convertBackJodaDateTypes(typeVal, valueVal);
            }
            if (data == null) {
                data = convertBackCustomEnumTypes(typeVal, valueVal);
            }
            if (data == null) {
                data = convertBackCustomTypes(typeVal, valueVal);
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

    private <T> byte[] convertCustomEnumTypes(T dataObject) {
        if (dataObject instanceof Enum) {
            return toBatchEeData(dataObject.getClass(), ((Enum) dataObject).name());
        }
        return null;
    }

    private <T> T convertBackCustomEnumTypes(String typeVal, String valueVal) {
        try {
            Class typeClass = getClassLoader().loadClass(typeVal);
            if (typeClass.isEnum()) {
                return (T) Enum.valueOf(typeClass, valueVal);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot convert back BatchEE data: " + valueVal + " of Enum type " + typeVal);
        }
        return null;
    }

    /**
     * This is an extension point for other serialisation algorithms.
     */
    protected <T> byte[] convertCustomTypes(T dataObject) {
        return null;
    }

    /**
     * This is an extension point for other serialsation algorithms.
     */
    private <T> T convertBackCustomTypes(String typeVal, String valueVal) {
        return null;
    }

    /**
     * This method converts java native types to a 'nice' string representation
     * which will be stored in the database.
     * @return the String representation or {@code null} if the dataObject was not a native Java type
     */
    protected <T> byte[] convertJavaNativeTypes(T dataObject) {
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

    protected <T> T convertBackJavaNativeTypes(String typeVal, String value) {
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
    protected <T> byte[] convertJava7DateTypes(T dataObject) {
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

    protected <T> T convertBackJava7DateTypes(String typeVal, String valueVal) {
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

    private <T> byte[] convertJodaDateTypes(T dataObject) {
        String className = dataObject.getClass().getName();
        if (className.equals("org.joda.time.LocalDate") ||
            className.equals("org.joda.time.LocalDateTime") ||
            className.equals("org.joda.time.LocalTime")) {
            // joda date API does the right thing on toString
            return toBatchEeData(dataObject.getClass(), dataObject.toString());
        }
        return null;
    }

    private <T> T convertBackJodaDateTypes(String typeVal, String valueVal) {
        if (typeVal.equals("org.joda.time.LocalDate") ||
            typeVal.equals("org.joda.time.LocalDateTime") ||
            typeVal.equals("org.joda.time.LocalTime")) {
            return (T) invokeStaticMethod(typeVal, "parse", String.class, valueVal);
        }
        return null;
    }

    private <T> byte[] convertJava8DateTypes(T dataObject) {
        String className = dataObject.getClass().getName();
        if (className.equals("java.time.LocalDate") ||
            className.equals("java.time.LocalDateTime") ||
            className.equals("java.time.LocalTime")) {
            // joda date API does the right thing on toString
            return toBatchEeData(dataObject.getClass(), dataObject.toString());
        }
        return null;
    }

    private <T> T convertBackJava8DateTypes(String typeVal, String valueVal) {
        if (typeVal.equals("java.time.LocalDate") ||
            typeVal.equals("java.time.LocalDateTime") ||
            typeVal.equals("java.time.LocalTime")) {
            return (T) invokeStaticMethod(typeVal, "parse", CharSequence.class, valueVal);
        }
        return null;
    }

    protected Object invokeStaticMethod(String typeVal, String methodName, Class paramType, String valueVal) {
        try {
            Class<?> typeClass = getClassLoader().loadClass(typeVal);
            Method method = typeClass.getMethod(methodName, paramType);
            return method.invoke(null, valueVal);
        } catch (Exception e) {
            throw new BatchContainerServiceException("Cannot convert data [" + valueVal + "] of type [" + typeVal + "]", e );
        }
    }

    /**
     * This is the default operation if no other way to serialise the data was used
     */
    protected <T> byte[] convertSerializableObjectTypes(T dataObject) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(dataObject);
        } catch (IOException e) {
            throw new BatchContainerServiceException("Cannot convert data for [" + dataObject.toString() + "]");
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                    // meh give up...
                }
            }
        }
        return baos.toByteArray();
    }


    protected <T> T convertBackSerializableObjectTypes(byte[] internalRepresentation) {
        final ByteArrayInputStream readerChkptBA = new ByteArrayInputStream(internalRepresentation);
        TCCLObjectInputStream readerOIS = null;
        try {
            // need to use the TCCL in case the batch is in a webapp but jbatch runtime is provided in a parent ClassLoader
            readerOIS = new TCCLObjectInputStream(readerChkptBA);
            T instance = (T) readerOIS.readObject();
            return instance;
        } catch (final Exception ex) {
            return null;
        } finally {
            if (readerOIS != null) {
                try {
                    readerOIS.close();
                } catch (IOException e) {
                    // meh give up...
                }
            }
        }
    }

    protected byte[] toBatchEeData(Class<?> type, String stringRepresentation) {
        return (BATCHEE_DATA_PREFIX + type.getName() + BATCHEE_SPLIT_TOKEN + stringRepresentation).getBytes(UTF8_CHARSET);
    }

    protected ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = this.getClass().getClassLoader();
        }
        return cl;
    }

    protected SimpleDateFormat getSimpleDateFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    }

    /**
     * Attention: The nanos must get concatenated separately as there is no formatter for nanos!
     */
    protected SimpleDateFormat getTimestampDateFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    }

}
