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
package com.github.workerframework.worker.testing.util;

import com.github.cafapi.common.api.CodecException;
import com.github.workerframework.worker.api.DataStoreException;

import java.io.FileNotFoundException;

/**
 * Created by ploch on 04/11/2015.
 */
public interface TaskFactory<TInput>
{
    byte[] createProduct(String taskId, TInput input) throws FileNotFoundException, DataStoreException, CodecException;
}
