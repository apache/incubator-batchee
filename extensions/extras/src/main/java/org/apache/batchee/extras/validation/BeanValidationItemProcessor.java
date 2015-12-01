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
package org.apache.batchee.extras.validation;

import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemProcessor;
import javax.inject.Inject;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Set;

@Documentation("Uses bean validation to validate incoming items.")
public class BeanValidationItemProcessor implements ItemProcessor {
    @Inject
    @BatchProperty
    @Documentation("optional bean validation group to validate")
    private String group;

    @Inject
    @BatchProperty
    @Documentation("whether a validation failure throws a ConstraintViolationException or just returns null")
    private String skipNotValidated;

    @Override
    public Object processItem(final Object item) throws Exception {
        final Validator validator = getValidator();
        final Set<ConstraintViolation<Object>> result = validate(validator, item);
        if (result != null && !result.isEmpty()) {
            if (Boolean.parseBoolean(skipNotValidated)) {
                return null;
            }
            throw new ConstraintViolationException(Set.class.cast(result));
        }
        return item;
    }

    private Set<ConstraintViolation<Object>> validate(final Validator validator, final Object item) throws ClassNotFoundException {
        final Set<ConstraintViolation<Object>> result;
        if (group == null) {
            result = validator.validate(item);
        } else {
            result = validator.validate(item, loadGroup());
        }
        return result;
    }

    protected Class<?> loadGroup() throws ClassNotFoundException {
        return Thread.currentThread().getContextClassLoader().loadClass(group);
    }

    protected Validator getValidator() throws NamingException {
        Validator validator;
        try {
            validator = Validator.class.cast(new InitialContext().lookup("java:comp/Validator"));
        } catch (final NamingException nnfe) {
            validator = Validation.buildDefaultValidatorFactory().getValidator();
        }
        return validator;
    }
}
