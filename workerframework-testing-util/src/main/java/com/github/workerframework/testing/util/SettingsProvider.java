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

/**
 * Created by ploch on 23/11/2015.
 */
//// TODO: 25/12/2015 Remove this class and use commons-configuration Configuration class instead.
public abstract class SettingsProvider
{
    public static final SystemSettingsProvider defaultProvider = new SystemSettingsProvider();

    public abstract String getSetting(String name);

    public boolean getBooleanSetting(String name)
    {
        return Boolean.parseBoolean(getSetting(name));
    }

    public boolean getBooleanSetting(String name, boolean defaultValue)
    {
        String setting = getSetting(name);
        if (setting != null) {
            return Boolean.parseBoolean(getSetting(name));
        }
        return defaultValue;
    }

}