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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.XAConnectionFactory;
import javax.naming.Context;
import javax.naming.NamingException;

import org.apache.cxf.common.logging.LogUtils;
import org.apache.cxf.service.model.EndpointInfo;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.listener.AbstractMessageListenerContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.support.destination.DestinationResolver;

/**
 * Factory to create JmsTemplates and JmsListeners from configuration and context information
 */
public final class JMSFactory {

    private static final Logger LOG = LogUtils.getL7dLogger(JMSFactory.class);

    /**
     * JVM/System property name holding the protocols a JNDI provider url may use.
     */
    private static final String CONFIGURED_JMS_PROTOCOLS = "jms.protocols";

    /**
     * Constant holding the default allowed jms protocols. A token that already ends with a colon
     * is used as a literal prefix, every other token stands for the "token://" url prefix.
     */
    private static final String DEFAULT_JMS_PROTOCOLS
        = "vm,tcp,nio,ssl,http,https,ws,wss,classpath:";

    private static final List<String> ALLOWED_PROTOCOLS = parseProtocols(getConfiguredProtocols());

    /**
     * JNDI environment properties that must not be supplied from untrusted input.
     * <p>
     * Note: {@code java.naming.factory.initial} (INITIAL_CONTEXT_FACTORY) is intentionally
     * absent. It is a first-class, documented configuration parameter - every JNDI based JMS
     * deployment sets it to the broker's context factory (e.g. ActiveMQ, Artemis, WebLogic).
     * It is safe to allow because any attempt to redirect lookups to a remote host via a
     * substitute factory still requires setting PROVIDER_URL to a non JMS scheme, which the
     * protocol allowlist check rejects independently.
     * </p>
     * <ul>
     *   <li>{@code java.naming.factory.object} - object factories reconstruct objects returned
     *       by a lookup and can deserialize remote payloads.</li>
     *   <li>{@code java.naming.factory.state} - state factories run during bind/rebind and
     *       can trigger arbitrary serialization logic.</li>
     *   <li>{@code java.naming.factory.url.pkgs} - injects packages for URL context factory
     *       resolution and could register a handler for an otherwise allowed scheme.</li>
     * </ul>
     */
    private static final Set<String> BLOCKED_ENV_KEYS
        = Collections.unmodifiableSet(new LinkedHashSet<String>(Arrays.asList(
            "java.naming.factory.object",
            "java.naming.factory.state",
            "java.naming.factory.url.pkgs")));

    private JMSFactory() {
    }

    private static String getConfiguredProtocols() {
        try {
            return System.getProperty(CONFIGURED_JMS_PROTOCOLS, DEFAULT_JMS_PROTOCOLS);
        } catch (SecurityException ex) {
            return DEFAULT_JMS_PROTOCOLS;
        }
    }

    /**
     * Turn a comma separated list of protocols into the list of prefixes a JNDI provider url is
     * allowed to start with. A token ending with a colon is taken as a literal prefix, any other
     * token is expanded by appending "://" to it.
     *
     * @param protocols comma separated list of protocols
     * @return the immutable list of allowed provider url prefixes
     */
    static List<String> parseProtocols(String protocols) {
        if (protocols == null) {
            return Collections.emptyList();
        }
        List<String> allowed = new ArrayList<String>();
        StringTokenizer tokenizer = new StringTokenizer(protocols, ",");
        while (tokenizer.hasMoreTokens()) {
            String protocol = tokenizer.nextToken().trim();
            if (protocol.length() > 0) {
                allowed.add(protocol.endsWith(":") ? protocol : protocol + "://");
            }
        }
        return Collections.unmodifiableList(allowed);
    }

    /**
     * Avoid unsafe protocols like ldap or rmi in a JNDI provider url. Looking up an object over
     * such a protocol lets the remote side decide which class is instantiated locally, so a
     * provider url coming from a jms uri or a wsdl must never reach the naming context.
     *
     * @param providerUrl the JNDI provider url to validate
     */
    static void checkProviderUrl(String providerUrl) {
        if (providerUrl == null || providerUrl.length() == 0) {
            return;
        }
        for (int i = 0; i < ALLOWED_PROTOCOLS.size(); i++) {
            if (providerUrl.startsWith(ALLOWED_PROTOCOLS.get(i))) {
                return;
            }
        }
        throw new IllegalArgumentException("Unsafe protocol in JNDI URL: " + providerUrl);
    }

    /**
     * Rejects lookup names that contain a URL scheme (e.g. ldap://, rmi://), which would
     * redirect the lookup to a remote server and enable JNDI injection.
     *
     * @param name the JNDI name that is about to be looked up
     */
    public static void validateJndiName(String name) {
        if (name != null && name.indexOf("://") >= 0) {
            throw new IllegalArgumentException("JNDI name must not contain a URL: " + name);
        }
    }

    /**
     * Validate the provider url and the properties of a JNDI environment.
     *
     * @param environment the JNDI environment to validate
     */
    static void checkJndiEnvironment(Properties environment) {
        if (environment == null) {
            return;
        }
        // Reject properties that could be used to redirect or hijack the JNDI lookup
        for (String blocked : BLOCKED_ENV_KEYS) {
            if (environment.containsKey(blocked)) {
                throw new IllegalArgumentException("Disallowed JNDI environment property: " + blocked);
            }
        }
        String providerUrl = environment.getProperty(Context.PROVIDER_URL);
        if (providerUrl == null) {
            Object value = environment.get(Context.PROVIDER_URL);
            providerUrl = value == null ? null : value.toString();
        }
        checkProviderUrl(providerUrl);
    }

    /**
     * Retreive connection factory from jndi, wrap it in a UserCredentialsConnectionFactoryAdapter,
     * set username and password and return the ConnectionFactory
     * 
     * @param jmsConfig
     * @param jndiConfig
     * @return
     */
    static ConnectionFactory getConnectionFactoryFromJndi(JMSConfiguration jmsConfig) {
        JNDIConfiguration jndiConfig = jmsConfig.getJndiConfig();
        if (jndiConfig == null) {
            return null;
        }
        String connectionFactoryName = jndiConfig.getJndiConnectionFactoryName();
        if (connectionFactoryName == null) {
            return null;
        }
        String userName = jndiConfig.getConnectionUserName();
        String password = jndiConfig.getConnectionPassword();
        // Reject unsafe protocols and URL style lookup names before the lookup is done. This is
        // deliberately kept outside of the try block so the failure is not wrapped into the
        // RuntimeException below.
        validateJndiName(connectionFactoryName);
        checkJndiEnvironment(jndiConfig.getEnvironment());
        if (jmsConfig.getJndiTemplate() != null) {
            checkJndiEnvironment(jmsConfig.getJndiTemplate().getEnvironment());
        }
        try {
            ConnectionFactory cf = (ConnectionFactory)jmsConfig.getJndiTemplate().
                lookup(connectionFactoryName);
            if (!(cf instanceof SingleConnectionFactory)) {
                UserCredentialsConnectionFactoryAdapter uccf = new UserCredentialsConnectionFactoryAdapter();
                uccf.setUsername(userName);
                uccf.setPassword(password);
                uccf.setTargetConnectionFactory(cf);
                cf = uccf;
            }
            
            return cf;
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create JmsTemplate from configuration information. Most settings are taken from jmsConfig. The QoS
     * settings in headers override the settings from jmsConfig
     * 
     * @param jmsConfig configuration information
     * @param messageProperties context headers
     * @return
     */
    public static JmsTemplate createJmsTemplate(JMSConfiguration jmsConfig,
                                                JMSMessageHeadersType messageProperties) {
        if (jmsConfig.getJmsTemplate() != null) {
            return jmsConfig.getJmsTemplate();
        }
        @SuppressWarnings("deprecation")
        JmsTemplate jmsTemplate = jmsConfig.isUseJms11() 
            ? new JmsTemplate() 
            : new org.springframework.jms.core.JmsTemplate102();
        jmsTemplate.setConnectionFactory(jmsConfig.getOrCreateWrappedConnectionFactory());
        jmsTemplate.setPubSubDomain(jmsConfig.isPubSubDomain());
        if (jmsConfig.getReceiveTimeout() != null) {
            jmsTemplate.setReceiveTimeout(jmsConfig.getReceiveTimeout());
        }
        long timeToLive = (messageProperties != null && messageProperties.isSetTimeToLive())
            ? messageProperties.getTimeToLive() : jmsConfig.getTimeToLive();
        jmsTemplate.setTimeToLive(timeToLive);
        int priority = (messageProperties != null && messageProperties.isSetJMSPriority())
            ? messageProperties.getJMSPriority() : jmsConfig.getPriority();
        jmsTemplate.setPriority(priority);
        int deliveryMode = (messageProperties != null && messageProperties.isSetJMSDeliveryMode())
            ? messageProperties.getJMSDeliveryMode() : jmsConfig.getDeliveryMode();
        jmsTemplate.setDeliveryMode(deliveryMode);
        jmsTemplate.setExplicitQosEnabled(jmsConfig.isExplicitQosEnabled());
        jmsTemplate.setSessionTransacted(jmsConfig.isSessionTransacted());
        if (jmsConfig.getDestinationResolver() != null) {
            jmsTemplate.setDestinationResolver(jmsConfig.getDestinationResolver());
        }
        return jmsTemplate;
    }
    /**
     * Create and start listener using configuration information from jmsConfig. Uses
     * resolveOrCreateDestination to determine the destination for the listener.
     * 
     * @param ei the EndpointInfo for the listener
     * @param jmsConfig configuration information
     * @param listenerHandler object to be called when a message arrives
     * @param destinationName null for temp dest or a destination name
     * @return
     */
    @SuppressWarnings("deprecation")
    public static AbstractMessageListenerContainer createJmsListener(EndpointInfo ei,
                                                                    JMSConfiguration jmsConfig,
                                                                    MessageListener listenerHandler,
                                                                    String destinationName) {
        
        if (jmsConfig.getMessageListenerContainer() != null) {
            AbstractMessageListenerContainer  jmsListener =  jmsConfig.getMessageListenerContainer();
            if (jmsListener.getMessageListener() == null) {
                jmsListener.setMessageListener(listenerHandler);
                jmsListener.initialize();
                jmsListener.start();
            }
            return jmsListener;
        }
        
        if (jmsConfig.getMessageListenerContainer() != null) {
            return jmsConfig.getMessageListenerContainer();
        }
        DefaultMessageListenerContainer jmsListener = null;
        
        if (jmsConfig.isUseJms11()) {
            //Check to see if transport is being used in JCA RA with XA
            Method method = ei.getProperty(JCATransactionalMessageListenerContainer.MDB_TRANSACTED_METHOD,
                                           java.lang.reflect.Method.class);
            if (method != null 
                && 
                jmsConfig.getConnectionFactory() instanceof XAConnectionFactory) {
                jmsListener = new JCATransactionalMessageListenerContainer(ei); 
            } else {
                jmsListener = new DefaultMessageListenerContainer();
            }
        } else {
            jmsListener = new org.springframework.jms.listener.DefaultMessageListenerContainer102();
        }
        
        return createJmsListener(jmsListener,
                                 jmsConfig,
                                 listenerHandler,
                                 destinationName,
                                 null, null, false);            
    }
    /**
     * Create and start listener using configuration information from jmsConfig. Uses
     * resolveOrCreateDestination to determine the destination for the listener.
     * 
     * @param jmsConfig configuration information
     * @param listenerHandler object to be called when a message arrives
     * @param destinationName null for temp dest or a destination name
     * @param messageSelectorPrefix prefix for the messageselector
     * @return
     */
    public static DefaultMessageListenerContainer createJmsListener(JMSConfiguration jmsConfig,
                                                                    MessageListener listenerHandler,
                                                                    Destination destination, 
                                                                    String messageSelectorPrefix,
                                                                    boolean userCID) {
        @SuppressWarnings("deprecation")
        DefaultMessageListenerContainer jmsListener = jmsConfig.isUseJms11()
            ? new DefaultMessageListenerContainer() 
            : new org.springframework.jms.listener.DefaultMessageListenerContainer102();
        
        return createJmsListener(jmsListener,
                                 jmsConfig,
                                 listenerHandler,
                                 null,
                                 destination, 
                                 messageSelectorPrefix,
                                 userCID);    
    }
    public static DefaultMessageListenerContainer createJmsListener(JMSConfiguration jmsConfig,
                                                                    MessageListener listenerHandler,
                                                                    String destination, 
                                                                    String messageSelectorPrefix,
                                                                    boolean userCID) {
        @SuppressWarnings("deprecation")
        DefaultMessageListenerContainer jmsListener = jmsConfig.isUseJms11()
            ? new DefaultMessageListenerContainer() 
            : new org.springframework.jms.listener.DefaultMessageListenerContainer102();
        
        return createJmsListener(jmsListener,
                                 jmsConfig,
                                 listenerHandler,
                                 destination,
                                 null, 
                                 messageSelectorPrefix,
                                 userCID);    
    }
    public static DefaultMessageListenerContainer createJmsListener(
                          DefaultMessageListenerContainer jmsListener,
                          JMSConfiguration jmsConfig,
                          MessageListener listenerHandler,
                          String destinationName,
                          Destination destination,
                          String messageSelectorPrefix,
                          boolean userCID) {
        
        jmsListener.setConcurrentConsumers(jmsConfig.getConcurrentConsumers());
        jmsListener.setMaxConcurrentConsumers(jmsConfig.getMaxConcurrentConsumers());
        jmsListener.setPubSubDomain(jmsConfig.isPubSubDomain());
        jmsListener.setPubSubNoLocal(jmsConfig.isPubSubNoLocal());
        
        jmsListener.setConnectionFactory(jmsConfig.getOrCreateWrappedConnectionFactory());
        jmsListener.setMessageSelector(jmsConfig.getMessageSelector());
        jmsListener.setSubscriptionDurable(jmsConfig.isSubscriptionDurable());
        jmsListener.setDurableSubscriptionName(jmsConfig.getDurableSubscriptionName());
        jmsListener.setSessionTransacted(jmsConfig.isSessionTransacted());
        jmsListener.setTransactionManager(jmsConfig.getTransactionManager());
        jmsListener.setMessageListener(listenerHandler);
        if (listenerHandler instanceof JMSDestination) {
            //timeout on server side?
            if (jmsConfig.getServerReceiveTimeout() != null) {
                jmsListener.setReceiveTimeout(jmsConfig.getServerReceiveTimeout());
            }
        } else if (jmsConfig.getReceiveTimeout() != null) {
            jmsListener.setReceiveTimeout(jmsConfig.getReceiveTimeout());
        }
        if (jmsConfig.getRecoveryInterval() != JMSConfiguration.DEFAULT_VALUE) {
            jmsListener.setRecoveryInterval(jmsConfig.getRecoveryInterval());
        }
        if (jmsConfig.getCacheLevelName() != null && (jmsConfig.getCacheLevelName().trim().length() > 0)) {
            jmsListener.setCacheLevelName(jmsConfig.getCacheLevelName());
        } else if (jmsConfig.getCacheLevel() != JMSConfiguration.DEFAULT_VALUE) {
            jmsListener.setCacheLevel(jmsConfig.getCacheLevel());
        }
        if (jmsListener.getCacheLevel() >= DefaultMessageListenerContainer.CACHE_CONSUMER
            && jmsConfig.getMaxSuspendedContinuations() > 0) {
            LOG.info("maxSuspendedContinuations value will be ignored - "
                     + ", please set cacheLevel to the value less than "
                     + " org.springframework.jms.listener.DefaultMessageListenerContainer.CACHE_CONSUMER");
        }
        if (jmsConfig.isAcceptMessagesWhileStopping()) {
            jmsListener.setAcceptMessagesWhileStopping(jmsConfig.isAcceptMessagesWhileStopping());
        }
        String staticSelectorPrefix = jmsConfig.getConduitSelectorPrefix();
        if (!userCID && messageSelectorPrefix != null && jmsConfig.isUseConduitIdSelector()) {
            jmsListener.setMessageSelector("JMSCorrelationID LIKE '" 
                                        + staticSelectorPrefix 
                                        + messageSelectorPrefix + "%'");
        } else if (staticSelectorPrefix.length() > 0) {
            jmsListener.setMessageSelector("JMSCorrelationID LIKE '" 
                                        + staticSelectorPrefix +  "%'");
        }
        
        if (jmsConfig.getDestinationResolver() != null) {
            jmsListener.setDestinationResolver(jmsConfig.getDestinationResolver());
        }
        if (jmsConfig.getTaskExecutor() != null) {
            setTaskExecutor(jmsListener, jmsConfig.getTaskExecutor());
        } 
        if (destination != null) {
            jmsListener.setDestination(destination);
        } else if (jmsConfig.isAutoResolveDestination()) {
            jmsListener.setDestinationName(destinationName);
        } else {
            JmsTemplate jmsTemplate = createJmsTemplate(jmsConfig, null);
            Destination dest = JMSFactory.resolveOrCreateDestination(jmsTemplate, destinationName, jmsConfig
                .isPubSubDomain());
            jmsListener.setDestination(dest);
        }
        jmsListener.initialize();
        jmsListener.start();
        return jmsListener;
    }
    
    private static void setTaskExecutor(DefaultMessageListenerContainer jmsListener, TaskExecutor exec) {
        //CXF-2630 - The method sig for DefaultMessageListenerContainer.setTaskExecutor changed between 
        //Spring 2.5 and 3.0 and code compiled for one won't run on the other.   Thus, we need
        //to revert to using some reflection to make this call
        Exception ex = null;
        for (Method m : jmsListener.getClass().getMethods()) {
            if ("setTaskExecutor".equals(m.getName())
                && m.getParameterTypes().length == 1
                && m.getParameterTypes()[0].isInstance(exec)) {
                try {
                    m.invoke(jmsListener, exec);
                    return;
                } catch (Exception e) {
                    ex = e;
                }
            }
        }
        //if we get here, we couldn't find a valid method or something else went wrong
        if (ex != null) {
            LOG.log(Level.WARNING, "ERROR_SETTING_TASKEXECUTOR", ex);
        } else {
            LOG.log(Level.WARNING, "NO_SETTASKEXECUTOR_METHOD", jmsListener.getClass().getName());
        }
    }

    /**
     * If the destinationName given is null then a temporary destination is created else the destination name
     * is resolved using the resolver from the jmsConfig
     * 
     * @param jmsTemplate template to use for session and resolver
     * @param replyToDestinationName null for temporary destination or a destination name
     * @param pubSubDomain true=pubSub, false=Queues
     * @return resolved destination
     */
    public static Destination resolveOrCreateDestination(final JmsTemplate jmsTemplate,
                                                          final String replyToDestinationName,
                                                          final boolean pubSubDomain) {
        return jmsTemplate.execute(new SessionCallback<Destination>() {
            public Destination doInJms(Session session) throws JMSException {
                if (replyToDestinationName == null) {
                    if (session instanceof QueueSession) {
                        // For JMS 1.0.2
                        return ((QueueSession)session).createTemporaryQueue();
                    } else {
                        // For JMS 1.1
                        return session.createTemporaryQueue();
                    }
                }
                DestinationResolver resolv = jmsTemplate.getDestinationResolver();
                return resolv.resolveDestinationName(session, replyToDestinationName, pubSubDomain);
            }
        });
    }

}
