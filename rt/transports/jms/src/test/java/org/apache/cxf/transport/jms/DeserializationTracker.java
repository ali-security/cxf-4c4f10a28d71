/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cxf.transport.jms;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.activemq.command.ActiveMQObjectMessage;

/**
 * Serializable payload which records whether it has been deserialized. It stands in for a
 * deserialization gadget to check that an inbound ObjectMessage body is never deserialized
 * unless ObjectMessages are explicitly allowed.
 */
public class DeserializationTracker implements Serializable {
    private static final long serialVersionUID = 1L;

    private static volatile boolean deserialized;

    public static void reset() {
        deserialized = false;
    }

    public static boolean isDeserialized() {
        return deserialized;
    }

    /**
     * Create an ObjectMessage which only holds the serialized form of the payload, like a
     * message received from the broker, so that ObjectMessage.getObject() deserializes it.
     */
    public static ObjectMessage createSerializedObjectMessage(Serializable payload) throws JMSException {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObject(payload);
        // the copy only carries the serialized content, the payload object itself is dropped
        return (ObjectMessage)message.copy();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        deserialized = true;
    }
}
