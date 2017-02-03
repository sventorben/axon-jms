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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.SimpleEventBus;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class JmsPublisherTest {

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private JmsPublisher cut;
  private TopicConnectionFactory connectionFactory;
  private TopicPublisher publisher;
  private Topic topic;
  private JmsMessageConverter converter;
  private SimpleEventBus eventBus;


  @Before
  public void setUp() throws Exception {
    eventBus = new SimpleEventBus();
    cut = new JmsPublisher(eventBus);
    connectionFactory = mock(TopicConnectionFactory.class);
    publisher = mock(TopicPublisher.class);
    topic = mock(Topic.class);
    converter = mock(JmsMessageConverter.class);
    cut.setConnectionFactory(connectionFactory);
    cut.setTopic(topic);
    cut.setTransacted(true);
    cut.setMessageConverter(converter);
    cut.postConstruct();
    cut.start();
  }

  @After
  public void tearDown() {
    while (CurrentUnitOfWork.isStarted()) {
      CurrentUnitOfWork.get().rollback();
    }
    cut.shutDown();
  }

  @Test
  public void testSendMessage_NoUnitOfWork() throws Exception {
    TopicConnection connection = mock(TopicConnection.class);
    when(connectionFactory.createTopicConnection()).thenReturn(connection);
    TopicSession transactionalSession = mock(TopicSession.class);
    when(connection.createTopicSession(true, Session.SESSION_TRANSACTED))
        .thenReturn(transactionalSession);
    when(transactionalSession.createPublisher(topic)).thenReturn(publisher);
    GenericEventMessage<String> message = new GenericEventMessage<>("Message");
    TextMessage jmsMessage = mock(TextMessage.class);
    when(converter.createJmsMessage(message, transactionalSession)).thenReturn(jmsMessage);
    eventBus.publish(message);

    verify(publisher).publish(jmsMessage);
    verify(transactionalSession).commit();
    verify(transactionalSession).close();
  }

  @Test
  public void testSendMessage_WithTransactionalUnitOfWork() throws Exception {
    GenericEventMessage<String> message = new GenericEventMessage<>("Message");
    final UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(message);

    TopicConnection connection = mock(TopicConnection.class);
    when(connectionFactory.createTopicConnection()).thenReturn(connection);
    TopicSession transactionalSession = mock(TopicSession.class);

    when(connection.createTopicSession(true, Session.SESSION_TRANSACTED))
        .thenReturn(transactionalSession);
    when(transactionalSession.createPublisher(topic)).thenReturn(publisher);
    when(transactionalSession.getTransacted()).thenReturn(true);
    TextMessage jmsMessage = mock(TextMessage.class);
    when(converter.createJmsMessage(message, transactionalSession)).thenReturn(jmsMessage);
    eventBus.publish(message);

    uow.commit();
    verify(publisher).publish(jmsMessage);
    verify(transactionalSession).commit();
    verify(transactionalSession).close();
  }


  @Test
  public void testSendMessage_WithTransactionalUnitOfWork_ChannelClosedBeforeCommit()
      throws Exception {
    GenericEventMessage<String> message = new GenericEventMessage<>("Message");
    final UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(message);

    TopicConnection connection = mock(TopicConnection.class);
    when(connectionFactory.createTopicConnection()).thenReturn(connection);
    TopicSession transactionalSession = mock(TopicSession.class);

    when(connection.createTopicSession(true, Session.SESSION_TRANSACTED))
        .thenReturn(transactionalSession);
    when(transactionalSession.createPublisher(topic)).thenReturn(publisher);
    when(transactionalSession.getTransacted()).thenReturn(true);
    TextMessage jmsMessage = mock(TextMessage.class);
    when(converter.createJmsMessage(message, transactionalSession)).thenReturn(jmsMessage);
    doThrow(JMSException.class).when(publisher).publish(jmsMessage);
    eventBus.publish(message);

    try {
      uow.commit();
      fail("Expected exception");
    } catch (EventPublicationFailedException ex) {
      assertNotNull(ex.getMessage());
    }
    verify(publisher).publish(jmsMessage);
    verify(transactionalSession, never()).commit();
    verify(transactionalSession).rollback();
    verify(transactionalSession).close();
  }

  @Test
  public void testSendMessage_WithUnitOfWorkRollback() throws Exception {
    GenericEventMessage<String> message = new GenericEventMessage<>("Message");
    final UnitOfWork<?> uow = DefaultUnitOfWork.startAndGet(message);

    TopicConnection connection = mock(TopicConnection.class);
    when(connectionFactory.createTopicConnection()).thenReturn(connection);
    TopicSession transactionalSession = mock(TopicSession.class);

    when(connection.createTopicSession(true, Session.SESSION_TRANSACTED))
        .thenReturn(transactionalSession);
    when(transactionalSession.createPublisher(topic)).thenReturn(publisher);
    when(transactionalSession.getTransacted()).thenReturn(true);
    TextMessage jmsMessage = mock(TextMessage.class);
    when(converter.createJmsMessage(message, transactionalSession)).thenReturn(jmsMessage);
    eventBus.publish(message);

    verify(transactionalSession, never()).rollback();
    verify(transactionalSession, never()).commit();
    verify(transactionalSession, never()).close();

    uow.rollback();
    verify(publisher, never()).publish(jmsMessage);
    verify(transactionalSession, never()).commit();
    verify(connectionFactory, never()).createTopicConnection();
  }

  @Test
  public void testSendPersistentMessage() throws Exception {
    cut.setPersistent(true);
    cut.setMessageConverter(null);
    cut.postConstruct();

    TopicConnection connection = mock(TopicConnection.class);
    when(connectionFactory.createTopicConnection()).thenReturn(connection);
    TopicSession transactionalSession = mock(TopicSession.class);

    when(connection.createTopicSession(true, Session.SESSION_TRANSACTED))
        .thenReturn(transactionalSession);
    when(transactionalSession.createPublisher(topic)).thenReturn(publisher);
    TextMessage jmsMessage = mock(TextMessage.class);
    when(transactionalSession.createTextMessage(any())).thenReturn(jmsMessage);
    ArgumentCaptor<Message> jmsMsgCapture = ArgumentCaptor.forClass(Message.class);
    doNothing().when(publisher).publish(jmsMsgCapture.capture());

    eventBus.publish(new GenericEventMessage<>("Message"));

    verify(jmsMessage).setJMSDeliveryMode(DeliveryMode.PERSISTENT);
  }

}
