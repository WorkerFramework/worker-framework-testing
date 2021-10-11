/*
 * Copyright 2015-2021 Micro Focus or one of its affiliates.
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
package com.hpe.caf.worker.core;

import com.hpe.caf.api.worker.QueueTaskMessage;
import com.hpe.caf.api.worker.TaskInformation;

class CoreTaskInformation implements TaskInformation
{
    private final TaskInformation taskInformation;
    private final QueueTaskMessage queueTaskMessage;

    public CoreTaskInformation(
        final TaskInformation taskInformation,
        final QueueTaskMessage queueTaskMessage
    )
    {
        this.taskInformation = taskInformation;
        this.queueTaskMessage = queueTaskMessage;
    }

    @Override
    public String getInboundMessageId()
    {
        return taskInformation.getInboundMessageId();
    }

    public TaskInformation getTaskInformation()
    {
        return taskInformation;
    }

    public QueueTaskMessage getQueueTaskMessage()
    {
        return queueTaskMessage;
    }
}