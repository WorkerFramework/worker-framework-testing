/*
 * Copyright 2022-2024 Open Text.
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
package com.github.workerframework.testing.data;

import com.github.cafapi.common.api.Codec;
import com.github.cafapi.common.util.ref.DataSourceException;
import com.github.cafapi.common.util.ref.ReferencedData;
import com.github.workerframework.api.DataStore;
import com.github.workerframework.api.DataStoreSource;

import java.io.InputStream;

/**
 * Created by ploch on 08/12/2015.
 */
public class ContentDataHelper
{
    public static InputStream retrieveReferencedData(DataStore dataStore, Codec codec, ReferencedData referencedData) throws DataSourceException
    {
        DataStoreSource source = new DataStoreSource(dataStore, codec);

        return referencedData.acquire(source);
    }
}
