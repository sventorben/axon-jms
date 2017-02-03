/*
 * Copyright (c) 2017. Sven-Torben Janus
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.sven_torben.axon.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;

import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DefaultJmsMessageConverterTest {

  @Rule
  public EmbeddedActiveMQBroker embeddedBroker = new EmbeddedActiveMQBroker();

  private DefaultJmsMessageConverter cut;

  private TopicSession topicSession;
  private TopicConnection topicConnection;
  private TopicConnectionFactory connectionFactory;

  @Before
  public void setUp() throws Exception {
    cut = new DefaultJmsMessageConverter(new XStreamSerializer());
    connectionFactory = embeddedBroker.createConnectionFactory();
    topicConnection = connectionFactory.createTopicConnection();
    topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  @After
  public void tearDown() throws JMSException {
    topicSession.close();
    topicConnection.close();
  }

  @Test
  public void testWriteAndReadEventMessage() throws Exception {
    EventMessage<?> eventMessage = GenericEventMessage.asEventMessage("SomePayload")
        .withMetaData(MetaData.with("key", "value"));
    TextMessage jmsMessage = cut.createJmsMessage(eventMessage, topicSession);
    EventMessage<?> actualResult = cut.readJmsMessage(jmsMessage)
        .orElseThrow(() -> new AssertionError("Expected valid message"));

    assertEquals(eventMessage.getIdentifier(), jmsMessage.getStringProperty("axon-message-id"));
    assertEquals(eventMessage.getIdentifier(), actualResult.getIdentifier());
    assertEquals(eventMessage.getMetaData(), actualResult.getMetaData());
    assertEquals(eventMessage.getPayload(), actualResult.getPayload());
    assertEquals(eventMessage.getPayloadType(), actualResult.getPayloadType());
    assertEquals(eventMessage.getTimestamp(), actualResult.getTimestamp());
  }

  @Test
  public void testMessageIgnoredIfNotAxonMessageIdPresent() throws JMSException {
    EventMessage<?> eventMessage = GenericEventMessage.asEventMessage("SomePayload")
        .withMetaData(MetaData.with("key", "value"));
    TextMessage jmsMessage = cut.createJmsMessage(eventMessage, topicSession);

    jmsMessage.setObjectProperty("axon-message-id", null);
    assertFalse(cut.readJmsMessage(jmsMessage).isPresent());
  }

  @Test
  public void testMessageIgnoredIfNotAxonMessageTypePresent() throws JMSException {
    EventMessage<?> eventMessage = GenericEventMessage.asEventMessage("SomePayload")
        .withMetaData(MetaData.with("key", "value"));
    TextMessage jmsMessage = cut.createJmsMessage(eventMessage, topicSession);

    jmsMessage.setObjectProperty("axon-message-type", null);
    assertFalse(cut.readJmsMessage(jmsMessage).isPresent());
  }

  @Test
  public void testWriteAndReadDomainEventMessage() throws Exception {
    DomainEventMessage<?> eventMessage = new GenericDomainEventMessage<>(
        "Stub", "1234", 1L, "Payload", MetaData.with("key", "value"));
    TextMessage jmsMessage = cut.createJmsMessage(eventMessage, topicSession);
    final EventMessage<?> actualResult = cut.readJmsMessage(jmsMessage)
        .orElseThrow(() -> new AssertionError("Expected valid message"));

    assertEquals(eventMessage.getIdentifier(), jmsMessage.getStringProperty("axon-message-id"));
    assertEquals("1234", jmsMessage.getStringProperty("axon-message-aggregate-id"));
    assertEquals(1L, jmsMessage.getLongProperty("axon-message-aggregate-seq"));

    assertTrue(actualResult instanceof DomainEventMessage);
    assertEquals(eventMessage.getIdentifier(), actualResult.getIdentifier());
    assertEquals(eventMessage.getMetaData(), actualResult.getMetaData());
    assertEquals(eventMessage.getPayload(), actualResult.getPayload());
    assertEquals(eventMessage.getPayloadType(), actualResult.getPayloadType());
    assertEquals(eventMessage.getTimestamp(), actualResult.getTimestamp());
    assertEquals(eventMessage.getAggregateIdentifier(),
        ((DomainEventMessage) actualResult).getAggregateIdentifier());
    assertEquals(eventMessage.getType(),
        ((DomainEventMessage) actualResult).getType());
    assertEquals(eventMessage.getSequenceNumber(),
        ((DomainEventMessage) actualResult).getSequenceNumber());
  }
}
