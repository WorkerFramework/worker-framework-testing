/*
 * Copyright 2022-2025 Open Text.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.workerframework.testing.validation;

import com.github.cafapi.common.api.Codec;
import com.github.workerframework.api.DataStore;

import java.util.Collection;

/**
 * Created by gibsodom on 07/01/2016.
 */
public class ArrayReferencedDataValidator extends PropertyValidator
{
    private DataStore dataStore;
    private Codec codec;
    private String testDataLocation;
    private String testSourcefileBaseFolder;

    public ArrayReferencedDataValidator(DataStore store, Codec codec, String testDataLocation, String testSourcefileBaseFolder)
    {
        this.dataStore = store;
        this.codec = codec;
        this.testDataLocation = testDataLocation;
        this.testSourcefileBaseFolder = testSourcefileBaseFolder;
    }

    @Override
    protected boolean isValid(Object testedPropertyValue, Object validatorPropertyValue)
    {
        if (testedPropertyValue == null && validatorPropertyValue == null) {
            return true;
        }
        if (!(testedPropertyValue instanceof Collection)) {
            return false;
        }
        if (!(validatorPropertyValue instanceof Collection)) {
            return false;
        }
        Object[] testedArray = ((Collection) testedPropertyValue).toArray();

        Object[] validationArray = ((Collection) validatorPropertyValue).toArray();

        for (int i = 0; i < validationArray.length; i++) {
            Object testedValue = testedArray[i];
            Object validationValue = validationArray[i];
            PropertyValidator validator = new ReferenceDataValidator(dataStore, codec, testDataLocation, testSourcefileBaseFolder); //validatorFactory.create(null,testedValue,validationValue);
            boolean valid = validator.isValid(testedValue, validationValue);
            if (!valid) {
                return false;
            }
        }

        return true;
    }
}
