/*
 * Copyright 2023 Open Text.
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
package com.hpe.caf.worker.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import com.hpe.caf.api.Codec;
import com.hpe.caf.api.CodecException;
import com.hpe.caf.api.DecodeMethod;
import com.hpe.caf.api.worker.QueueTaskMessage;
import com.hpe.caf.api.worker.TaskMessage;
import com.hpe.caf.util.rabbitmq.Delivery;
import com.hpe.caf.util.rabbitmq.Event;
import com.hpe.caf.util.rabbitmq.QueueConsumer;
import com.rabbitmq.client.Channel;

/**
 * Created by ploch on 01/11/2015.
 */
public class SimpleQueueConsumerImpl<T> implements QueueConsumer
{
    private final BlockingQueue<Event<QueueConsumer>> eventQueue;
    private final Channel channel;
    private final ResultHandler<T> resultHandler;
    private final Codec codec;
    private final ArrayList<Delivery> deliveries = new ArrayList<>();

    private static final Object syncLock = new Object();

    public SimpleQueueConsumerImpl(final BlockingQueue<Event<QueueConsumer>> queue, Channel channel,
                                   ResultHandler<T> resultHandler, final Codec codec)
    {
        this.eventQueue = queue;
        this.channel = channel;
        this.resultHandler = resultHandler;
        this.codec = codec;
    }

    @Override
    public void processDelivery(Delivery delivery)
    {

        System.out.print("New delivery");

        try {
            deserialize(delivery);
        } catch (CodecException e) {
            e.printStackTrace();
            throw new AssertionError("Failed: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AssertionError("Failed: " + e.getMessage());
        }

    }

    private void deserialize(final Delivery delivery) throws Exception
    {
        try {
            final TaskMessage taskMessage = codec.deserialise(delivery.getMessageData(), TaskMessage.class, DecodeMethod.LENIENT);
            System.out.println(taskMessage.getTaskId() + ", status: " + taskMessage.getTaskStatus());
            synchronized(syncLock) {
                resultHandler.handleResult((T)taskMessage);
            }
            return;
        } catch (final Exception ignored){}
        try {
            final QueueTaskMessage taskMessage =
                    codec.deserialise(delivery.getMessageData(), QueueTaskMessage.class, DecodeMethod.LENIENT);
            System.out.println(taskMessage.getTaskId() + ", status: " + taskMessage.getTaskStatus());
            synchronized(syncLock) {
                resultHandler.handleResult((T)taskMessage);
            }
        } catch (final Exception e){
            throw new Exception("Invalid data received. It should be a QueueTaskMessage or a TaskMessage.");
        }
    }

    @Override
    public void processAck(long tag)
    {
        try {
            channel.basicAck(tag, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processReject(long tag)
    {
    }

    @Override
    public void processDrop(long tag)
    {
    }
}
