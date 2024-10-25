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
package com.github.workerframework.testing.util;

import com.github.cafapi.common.api.BootstrapConfiguration;
import com.github.cafapi.common.api.Cipher;
import com.github.cafapi.common.api.CipherException;
import com.github.cafapi.common.api.CipherProvider;
import com.github.cafapi.common.api.Codec;
import com.github.cafapi.common.api.ConfigurationDecoderProvider;
import com.github.cafapi.common.api.ConfigurationException;
import com.github.cafapi.common.api.ConfigurationSource;
import com.github.cafapi.common.api.ConfigurationSourceProvider;
import com.github.cafapi.common.api.Decoder;
import com.github.cafapi.common.bootstrapconfigs.system.SystemBootstrapConfiguration;
import com.github.cafapi.common.ciphers.Null.NullCipherProvider;
import com.github.cafapi.common.config.decoder.CafConfigurationDecoderProvider;
import com.github.cafapi.common.util.moduleloader.ModuleLoader;
import com.github.cafapi.common.util.moduleloader.ModuleLoaderException;
import com.github.cafapi.common.util.naming.ServicePath;
import com.github.workerframework.api.DataStore;
import com.github.workerframework.api.DataStoreException;
import com.github.workerframework.api.DataStoreProvider;
import com.github.workerframework.datastores.http.HttpDataStoreProvider;

/**
 * Created by ploch on 22/10/2015.
 */
public class WorkerServicesFactory
{
    private static BootstrapConfiguration bootstrapConfiguration = new SystemBootstrapConfiguration();

    private WorkerServicesFactory()
    {
    }

    public static WorkerServices create() throws ModuleLoaderException, CipherException, ConfigurationException, DataStoreException
    {

        Codec codec = ModuleLoader.getService(Codec.class);
        ConfigurationDecoderProvider decoderProvider = ModuleLoader.getService(ConfigurationDecoderProvider.class,
                                                                               CafConfigurationDecoderProvider.class);
        Decoder decoder = decoderProvider.getDecoder(bootstrapConfiguration, codec);
        Cipher cipher = ModuleLoader.getService(CipherProvider.class, NullCipherProvider.class).getCipher(bootstrapConfiguration);
        ServicePath path = bootstrapConfiguration.getServicePath();
        ConfigurationSource configurationSource = ModuleLoader.getService(ConfigurationSourceProvider.class).getConfigurationSource(bootstrapConfiguration, cipher, path, decoder);
        final DataStore dataStore = new SystemSettingsProvider().getBooleanSetting("worker.testing.usehttpdatastore")
            ? new HttpDataStoreProvider().getDataStore(configurationSource)
            : ModuleLoader.getService(DataStoreProvider.class).getDataStore(configurationSource);

        return new WorkerServices(bootstrapConfiguration, codec, cipher, configurationSource, dataStore);
    }
}
