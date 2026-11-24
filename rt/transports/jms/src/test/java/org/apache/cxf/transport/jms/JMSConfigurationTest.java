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

import java.util.List;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.naming.Context;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.cxf.transport.jms.uri.JMSEndpoint;
import org.apache.cxf.transport.jms.uri.JMSEndpointParser;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jndi.JndiTemplate;

public class JMSConfigurationTest extends Assert {

    @Test
    public void testDestroyAutoWrappedConnectionFactory() {
        // create an empty configuration
        JMSConfiguration jmsConfig = new JMSConfiguration();
        // create the connectionFactory to wrap
        ActiveMQConnectionFactory amqCf = new ActiveMQConnectionFactory();
        jmsConfig.setConnectionFactory(amqCf);
        // get the connectionFactory
        assertTrue("Should get the instance of ActiveMQConnectionFactory",
                   jmsConfig.getConnectionFactory() instanceof ActiveMQConnectionFactory);
        // get the wrapped connectionFactory
        ConnectionFactory wrappingCf = jmsConfig.getOrCreateWrappedConnectionFactory();
        assertTrue("Should get the instance of SingleConnectionFactory",
                   wrappingCf instanceof SingleConnectionFactory);
        SingleConnectionFactory scf = (SingleConnectionFactory)wrappingCf;
        assertSame("Should get the wrapped ActiveMQConnectionFactory",
                    amqCf, scf.getTargetConnectionFactory());
        // destroy the wrapping
        jmsConfig.destroyWrappedConnectionFactory();
        // get the wrapping cf
        assertNull("Should be null after destroy", jmsConfig.getWrappedConnectionFactory());
        // original connectionFactory should be unchanged
        assertSame("Should be the same with original connectionFactory",
                    amqCf, jmsConfig.getConnectionFactory());
    }

    @Test
    public void testDestroySuppliedConnectionFactory() {
        // create an empty configuration
        JMSConfiguration jmsConfig = new JMSConfiguration();
        // create the connectionFactory to wrap
        ActiveMQConnectionFactory amqCf = new ActiveMQConnectionFactory();
        // wrap into SingleConnectionFactory
        SingleConnectionFactory scf = new SingleConnectionFactory(amqCf);
        // set the connectionFactory to reuse
        jmsConfig.setConnectionFactory(scf);
        // get the connectionFactory
        assertTrue("Should get the instance of SingleConnectionFactory",
                   jmsConfig.getConnectionFactory() instanceof SingleConnectionFactory);
        // get the wrapped connectionFactory
        ConnectionFactory wrappingCf = jmsConfig.getOrCreateWrappedConnectionFactory();
        assertTrue("Should get the instance of SingleConnectionFactory",
                   wrappingCf instanceof SingleConnectionFactory);
        assertSame("Should not be wrapped again",
                    scf, wrappingCf);
        // destroy the wrapping
        jmsConfig.destroyWrappedConnectionFactory();
        // get the wrapping cf
        assertNotNull("Should be not null after destroy", jmsConfig.getWrappedConnectionFactory());
        // original connectionFactory should be unchanged
        assertSame("Should be the same with supplied connectionFactory",
                    scf, jmsConfig.getConnectionFactory());
    }

    @Test
    public void testJndiForbiddenProtocol() throws Exception {
        JNDIConfiguration jndiConfig = new JNDIConfiguration();
        try {
            jndiConfig.setEnvironment(createLdapEnvironment());
            fail("JNDI lookup should have failed");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }

    @Test
    public void testJndiForbiddenProtocolInJndiTemplate() throws Exception {
        JndiTemplate jt = new JndiTemplate();
        jt.setEnvironment(createLdapEnvironment());

        JMSConfiguration jmsConfig = new JMSConfiguration();
        try {
            jmsConfig.setJndiTemplate(jt);
            fail("JNDI lookup should have failed");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }

    @Test
    public void testJndiForbiddenProtocolOnConnectionFactoryLookup() throws Exception {
        JNDIConfiguration jndiConfig = new JNDIConfiguration();
        jndiConfig.setJndiConnectionFactoryName("objectName");

        JMSConfiguration jmsConfig = new JMSConfiguration();
        jmsConfig.setJndiConfig(jndiConfig);
        // The environment is only filled in after the template was handed over so the check
        // on the setter can not see it. The lookup itself has to refuse it as well.
        JndiTemplate jt = new JndiTemplate();
        jmsConfig.setJndiTemplate(jt);
        jt.setEnvironment(createLdapEnvironment());

        try {
            JMSFactory.getConnectionFactoryFromJndi(jmsConfig);
            fail("JNDI lookup should have failed");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }

    @Test
    public void testJndiForbiddenProtocolFromJmsUri() throws Exception {
        JMSEndpoint endpoint = JMSEndpointParser
            .createEndpoint("jms:jndi:Foo.Bar?jndiInitialContextFactory="
                            + "com.sun.jndi.rmi.registry.RegistryContextFactory"
                            + "&jndiConnectionFactoryName=ConnectionFactory"
                            + "&jndiURL=rmi://127.0.0.1:1099/Object");
        try {
            JMSOldConfigHolder.getInitialContextEnv(endpoint);
            fail("Unsafe jndiURL should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }

    @Test
    public void testJndiForbiddenProtocolFromJmsUriJndiParameter() throws Exception {
        JMSEndpoint endpoint = JMSEndpointParser
            .createEndpoint("jms:jndi:Foo.Bar?jndiConnectionFactoryName=ConnectionFactory"
                            + "&jndi-java.naming.factory.initial=com.sun.jndi.ldap.LdapCtxFactory"
                            + "&jndi-java.naming.provider.url=ldap://127.0.0.1:12345/o=Exploit");
        try {
            JMSOldConfigHolder.getInitialContextEnv(endpoint);
            fail("Unsafe jndi-java.naming.provider.url should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }

    @Test
    public void testJndiForbiddenProtocolFromNamingProperty() throws Exception {
        JMSEndpoint endpoint = new JMSEndpoint();
        endpoint.putJndiParameter(Context.PROVIDER_URL, "ldap://127.0.0.1:12345");
        try {
            JMSOldConfigHolder.getInitialContextEnv(endpoint);
            fail("Unsafe java.naming.provider.url should have been rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }

    @Test
    public void testUnsafeJndiProtocolsAreRejected() {
        assertUnsafeProviderUrl("ldap://127.0.0.1:12345/o=Exploit");
        assertUnsafeProviderUrl("ldaps://127.0.0.1:12345/o=Exploit");
        assertUnsafeProviderUrl("rmi://127.0.0.1:1099/Object");
        assertUnsafeProviderUrl("iiop://127.0.0.1:1050/Object");
        assertUnsafeProviderUrl("dns://127.0.0.1/example.com");
    }

    @Test
    public void testSafeJndiProtocolsAreAccepted() throws Exception {
        // The protocols used by the JMS brokers have to keep working
        JNDIConfiguration tcpConfig = new JNDIConfiguration();
        tcpConfig.setEnvironment(createJndiEnvironment("tcp://localhost:61616"));
        assertNotNull(tcpConfig.createJndiTemple());

        JNDIConfiguration vmConfig = new JNDIConfiguration();
        vmConfig.setEnvironment(createJndiEnvironment("vm://localhost"));
        assertNotNull(vmConfig.createJndiTemple());

        // A local spring resource is not a remote lookup so it stays allowed
        JNDIConfiguration classpathConfig = new JNDIConfiguration();
        classpathConfig.setEnvironment(createJndiEnvironment("classpath:/jms_test_jndi.xml"));
        assertNotNull(classpathConfig.createJndiTemple());

        // No provider url at all is valid as well
        assertNotNull(new JNDIConfiguration().createJndiTemple());
    }

    @Test
    public void testParseProtocols() {
        List<String> protocols = JMSFactory.parseProtocols("vm, tcp ,,classpath:");
        assertEquals(3, protocols.size());
        assertEquals("vm://", protocols.get(0));
        assertEquals("tcp://", protocols.get(1));
        assertEquals("classpath:", protocols.get(2));
        assertTrue(JMSFactory.parseProtocols(null).isEmpty());
        assertTrue(JMSFactory.parseProtocols("").isEmpty());
    }

    private Properties createJndiEnvironment(String providerUrl) {
        Properties env = new Properties();
        env.put(Context.PROVIDER_URL, providerUrl);
        return env;
    }

    private Properties createLdapEnvironment() {
        Properties env = createJndiEnvironment("ldap://127.0.0.1:12345");
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        // Allow following referrals (important for LDAP injection)
        env.put(Context.REFERRAL, "follow");
        return env;
    }

    private void assertUnsafeProviderUrl(String providerUrl) {
        try {
            JMSFactory.checkJndiEnvironment(createJndiEnvironment(providerUrl));
            fail("Provider url should have been rejected: " + providerUrl);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsafe protocol in JNDI URL"));
        }
    }
}
