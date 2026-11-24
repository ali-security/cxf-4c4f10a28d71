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
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;

import org.apache.cxf.helpers.CastUtils;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.message.MessageImpl;
import org.apache.cxf.security.SecurityContext;
import org.apache.cxf.transport.jms.spec.JMSSpecConstants;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.JmsException;

public class JMSUtilsTest extends Assert {

    @Test
    public void testObjectMessageIsRejectedByDefault() throws Exception {
        ObjectMessage objectMessage =
            DeserializationTracker.createSerializedObjectMessage(new DeserializationTracker());
        DeserializationTracker.reset();
        org.apache.cxf.message.Message inMessage = new MessageImpl();
        try {
            JMSUtils.retrieveAndSetPayload(inMessage, objectMessage, null);
            fail("Expected the ObjectMessage to be rejected");
        } catch (JmsException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ObjectMessage is disabled by configuration"));
        }
        assertFalse("The ObjectMessage body must not be deserialized", DeserializationTracker.isDeserialized());
        assertNull(inMessage.getContent(InputStream.class));
        assertNull(inMessage.getContent(Reader.class));
    }

    @Test
    public void testObjectMessageIsRejectedWhenNotAllowed() throws Exception {
        ObjectMessage objectMessage =
            DeserializationTracker.createSerializedObjectMessage(new DeserializationTracker());
        DeserializationTracker.reset();
        try {
            JMSUtils.retrieveAndSetPayload(new MessageImpl(), objectMessage, null, false);
            fail("Expected the ObjectMessage to be rejected");
        } catch (JmsException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ObjectMessage is disabled by configuration"));
        }
        assertFalse("The ObjectMessage body must not be deserialized", DeserializationTracker.isDeserialized());
    }

    @Test
    public void testObjectMessageIsNotReadByDefault() throws Exception {
        // the body accessor must not be called at all
        ObjectMessage objectMessage = EasyMock.createMock(ObjectMessage.class);
        EasyMock.replay(objectMessage);
        try {
            JMSUtils.retrieveAndSetPayload(new MessageImpl(), objectMessage, null);
            fail("Expected the ObjectMessage to be rejected");
        } catch (JmsException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ObjectMessage is disabled by configuration"));
        }
        EasyMock.verify(objectMessage);
    }

    @Test
    public void testObjectMessageCanBeEnabled() throws Exception {
        byte[] payload = "payload".getBytes("UTF-8");
        org.apache.cxf.message.Message inMessage = new MessageImpl();
        JMSUtils.retrieveAndSetPayload(inMessage, DeserializationTracker.createSerializedObjectMessage(payload),
                                       null, true);
        assertEquals(Collections.singletonList(JMSConstants.BYTE_MESSAGE_TYPE),
                     getProtocolHeaders(inMessage).get(JMSSpecConstants.JMS_MESSAGE_TYPE));
        InputStream in = inMessage.getContent(InputStream.class);
        assertNotNull(in);
        assertEquals("payload", IOUtils.toString(in, "UTF-8"));

        // when explicitly allowed the body is deserialized again
        DeserializationTracker.reset();
        JMSUtils.retrieveAndSetPayload(new MessageImpl(),
                                       DeserializationTracker
                                           .createSerializedObjectMessage(new DeserializationTracker()),
                                       null, true);
        assertTrue(DeserializationTracker.isDeserialized());
    }

    @Test
    public void testStreamMessageIsReportedAsUnsupportedByDefault() throws Exception {
        // no body accessor (readObject, readString, readBytes, ...) must be called
        StreamMessage streamMessage = EasyMock.createMock(StreamMessage.class);
        EasyMock.replay(streamMessage);

        org.apache.cxf.message.Message inMessage = new MessageImpl();
        JMSUtils.retrieveAndSetPayload(inMessage, streamMessage, null);

        assertEquals(Collections.singletonList("stream"),
                     getProtocolHeaders(inMessage).get(JMSSpecConstants.JMS_MESSAGE_TYPE));
        assertNull(inMessage.getContent(Reader.class));
        EasyMock.verify(streamMessage);
    }

    private static Map<String, List<String>> getProtocolHeaders(org.apache.cxf.message.Message inMessage) {
        return CastUtils.cast((Map<?, ?>)inMessage.get(org.apache.cxf.message.Message.PROTOCOL_HEADERS));
    }

    @Test
    public void testGetEncoding() throws IOException {                
        assertEquals("Get the wrong encoding", JMSUtils.getEncoding("text/xml; charset=utf-8"), "UTF-8");
        assertEquals("Get the wrong encoding", JMSUtils.getEncoding("text/xml"), "UTF-8");
        assertEquals("Get the wrong encoding", JMSUtils.getEncoding("text/xml; charset=GBK"), "GBK");
        try {
            JMSUtils.getEncoding("text/xml; charset=asci");
            fail("Expect the exception here");
        } catch (Exception ex) {
            assertTrue("we should get the UnsupportedEncodingException here",
                       ex instanceof UnsupportedEncodingException);
        }
    }
    
    @Test
    public void testCreateUserSecurityContext() throws Exception {
        Message service = new MessageStub();
        JMSConfiguration config = new JMSConfiguration();
        config.setCreateSecurityContext(true);
        
        org.apache.cxf.message.Message inMessage = new MessageImpl();
        
        JMSUtils.populateIncomingContext(service, inMessage, "text", config);
        assertNotNull(inMessage.get(SecurityContext.class));
        
        inMessage = new MessageImpl();
        config.setCreateSecurityContext(false);
        JMSUtils.populateIncomingContext(service, inMessage, "text", config);
        assertNull(inMessage.get(SecurityContext.class));
    }
    
    @Test
    public void testCorrelationIDGeneration() {
        final String conduitId = UUID.randomUUID().toString().replaceAll("-", "");
        // test min edge case
        AtomicLong messageMinCount = new AtomicLong(0);
        String correlationID = 
            JMSUtils.createCorrelationId(conduitId, messageMinCount.get());
        
        String expected = conduitId + "0000000000000000";
        assertEquals("The correlationID value does not match expected value",
                     expected, correlationID);
        assertEquals("The correlationID value does not match expected length",
                     48, correlationID.length());
        
        // test max edge case
        AtomicLong messageMaxCount = new AtomicLong(0xFFFFFFFFFFFFFFFFL);
        
        correlationID = 
            JMSUtils.createCorrelationId(conduitId, messageMaxCount.get());
        
        expected = conduitId + "ffffffffffffffff";
        assertEquals("The correlationID value does not match expected value",
                     expected, correlationID);
        assertEquals("The correlationID value does not match expected length",
                48, correlationID.length());

        // test overflow case
        AtomicLong overflowCount = new AtomicLong(0xFFFFFFFFFFFFFFFFL);
        
        correlationID = 
            JMSUtils.createCorrelationId(conduitId, overflowCount.incrementAndGet());
        
        expected = conduitId + "0000000000000000";
        assertEquals("The correlationID value does not match expected value",
                     expected, correlationID);
        assertEquals("The correlationID value does not match expected length",
                48, correlationID.length());
        
        // test sequential flow
        AtomicLong messageSequenceCount = new AtomicLong(0);
        correlationID = 
            JMSUtils.createCorrelationId(conduitId, messageSequenceCount.incrementAndGet());
        
        expected = conduitId + "0000000000000001";
        assertEquals("The correlationID value does not match expected value",
                     expected, correlationID);
        assertEquals("The correlationID value does not match expected length",
                     48, correlationID.length());

        correlationID = 
            JMSUtils.createCorrelationId(conduitId, messageSequenceCount.incrementAndGet());

        expected = conduitId + "0000000000000002";
        assertEquals("The correlationID value does not match expected value",
                     expected, correlationID);
        assertEquals("The correlationID value does not match expected length",
                     48, correlationID.length());
    }
}
