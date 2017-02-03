/*
 * The MIT License
 *
 * Copyright 2017 Sven-Torben Janus.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package de.sven_torben.axon.jms;

import java.util.UUID;
import java.util.function.Consumer;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JmsMessageSourceTest {

    @Rule
    public EmbeddedActiveMQBroker embeddedBroker = new EmbeddedActiveMQBroker();

    private JmsMessageSource cut;

    private TopicPublisher publisher;

    private TopicSession topicSession;

    private MessageConsumer consumer;

    private TopicConnection topicConnection;

    private DefaultJmsMessageConverter converter = new DefaultJmsMessageConverter(new XStreamSerializer());;

    Object oga;

    @Before
    public void setup() throws JMSException {
        
        final ActiveMQConnectionFactory connectionFactory = embeddedBroker.createConnectionFactory();
        topicConnection = connectionFactory.createTopicConnection();

        topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        embeddedBroker.start();
        topicConnection.start();
        
        final Topic topic = topicSession.createTemporaryTopic();
       
        publisher = topicSession.createPublisher(topic);
        consumer = topicSession.createConsumer(topic);
        
        cut = new JmsMessageSource(consumer, converter);
    }

    @After
    public void tearDown() throws JMSException {
        topicConnection.stop();
        embeddedBroker.stop();
    }

    @Test
    public void messageGetsPublished() throws JMSException, InterruptedException {
        final TestConsumer testConsumer = new TestConsumer();
        cut.subscribe(testConsumer);

        EventMessage<?> eventMessage = GenericEventMessage
                .asEventMessage("SomePayload")
                .withMetaData(MetaData.with("key", "value"));
        
        TextMessage jmsMessage = converter.createJmsMessage(eventMessage, topicSession);
        
        publisher.publish(jmsMessage);

        Thread.sleep(1000L);

        assertNotNull(testConsumer.latest);
    }

    private class TestConsumer implements Consumer<Object> {

        private Object latest;

        @Override
        public void accept(Object t) {
            this.latest = t;
        }
    }

}
