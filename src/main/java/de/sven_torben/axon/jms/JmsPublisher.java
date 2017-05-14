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

import java.util.List;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.naming.InitialContext;

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publisher implementation that uses JMS compatible Message Broker to dispatch event messages. All
 * outgoing messages are sent to a configured topic, which defaults to "{@code Axon.EventBus}".
 *
 * <p>This publisher does not dispatch Events internally, as it relies on each event processor to
 * listen to it's own JMS Queue.
 *
 * @author Sven-Torben Janus
 */
public class JmsPublisher {

  private static final Logger logger = LoggerFactory.getLogger(JmsPublisher.class);
  private static final String DEFAULT_TOPIC_NAME = "Axon.EventBus";
  private static final String DEFAULT_CONNECTION_FACTORY_NAME = "java:/ConnectionFactory";

  private final SubscribableMessageSource<EventMessage<?>> messageSource;

  private TopicConnectionFactory connectionFactory;
  private String topicName = DEFAULT_TOPIC_NAME;
  private Topic topic;
  private String connectionFactoryName = DEFAULT_CONNECTION_FACTORY_NAME;
  private boolean isTransacted = false;
  private boolean isPersitent = true;
  private JmsMessageConverter messageConverter;
  private Serializer serializer;
  private Registration eventBusRegistration;


  /**
   * Initialize this instance to publish message as they are published on the given
   * {@code messageSource}.
   *
   * <p>Uses the default ConnectionFactory {@code java:/ConnectionFactory} to establish connections
   * to the JMS message broker.
   *
   * @param messageSource The component providing messages to be published
   */
  public JmsPublisher(SubscribableMessageSource<EventMessage<?>> messageSource) {
    this.messageSource = messageSource;
  }

  /**
   * Subscribes this publisher to the messageSource provided during initialization.
   */
  public void start() {
    eventBusRegistration = messageSource.subscribe(this::send);
  }

  /**
   * Shuts down this component and unsubscribes it from its messageSource.
   */
  public void shutDown() {
    if (eventBusRegistration != null) {
      eventBusRegistration.cancel();
    }
  }

  /**
   * Sends the given {@code events} to the configured JMS Topic. It takes the current Unit of Work
   * into account when available. Otherwise, it simply publishes directly.
   *
   * @param events the events to publish on the JMS Message Broker
   */
  protected void send(List<? extends EventMessage<?>> events) {
    try (TopicConnection topicConnection = connectionFactory.createTopicConnection()) {
      int ackMode = isTransacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE;
      TopicSession topicSession = topicConnection.createTopicSession(isTransacted, ackMode);
      try (TopicPublisher publisher = topicSession.createPublisher(topic)) {
        for (EventMessage event : events) {
          Message jmsMessage = messageConverter.createJmsMessage(event, topicSession);
          doSendMessage(publisher, jmsMessage);
        }
      } finally {
        handleTransaction(topicSession);
      }
    } catch (JMSException ex) {
      throw new EventPublicationFailedException(
          "Unable to establish TopicConnection to JMS message broker.", ex);
    }
  }

  private void handleTransaction(final TopicSession topicSession) {
    try {

      if (CurrentUnitOfWork.isStarted() && isTransacted) {
        UnitOfWork<?> unitOfWork = CurrentUnitOfWork.get();
        unitOfWork.afterCommit(u -> {
          try {
            topicSession.commit();
          } catch (JMSException ex) {
            throw new EventPublicationFailedException(
                "Failed to commit transaction on topic session.", ex);
          }
          tryClose(topicSession);
        });
        unitOfWork.onRollback(u -> {
          try {
            topicSession.rollback();
          } catch (JMSException ex) {
            logger.warn("Unable to rollback transaction on topic session.", ex);
          }
          tryClose(topicSession);
        });
      } else if (isTransacted) {
        topicSession.commit();
      }
    } catch (JMSException ex) {
      if (isTransacted) {
        tryRollback(topicSession);
      }
      throw new EventPublicationFailedException(
          "Failed to dispatch events to the message broker.", ex);
    } finally {
      if (!CurrentUnitOfWork.isStarted()) {
        tryClose(topicSession);
      }
    }
  }

  private void tryClose(Session session) {
    try {
      session.close();
    } catch (JMSException ex) {
      logger.info("Unable to close session. It might already be closed.", ex);
    }
  }

  /**
   * Does the actual publishing of the given {@code jmsMessage} on the given {@code topicPublisher}.
   * This method can be overridden to change the properties used to send a message.
   *
   * @param topicPublisher The publisher used to dispatch the message
   * @param jmsMessage     The JMS Message to publish
   * @throws JMSException when an error occurs while writing the message
   */
  protected void doSendMessage(TopicPublisher topicPublisher, Message jmsMessage)
      throws JMSException {
    topicPublisher.publish(jmsMessage);
  }

  private void tryRollback(Session session) {
    try {
      session.rollback();
    } catch (JMSException ex) {
      logger.debug("Unable to rollback. The underlying session might already be closed.", ex);
    }
  }

  /**
   * Sets sensible defaults for dependent components like ConnectionFactory, Topic,
   * and MessageConverter.
   *
   * @throws Exception In case setting the defaults fails.
   */
  @PostConstruct
  public void postConstruct() throws Exception {
    InitialContext initialContext = new InitialContext();
    if (connectionFactory == null) {
      connectionFactory = (TopicConnectionFactory) initialContext.lookup(connectionFactoryName);
    }
    if (topic == null) {
      topic = (Topic) initialContext.lookup(topicName);
    }
    if (messageConverter == null) {
      messageConverter = new DefaultJmsMessageConverter(new XStreamSerializer(), isPersitent);
    }
  }

  /**
   * Whether this Publisher should dispatch its events in a transaction or not. Defaults to
   * {@code false}.
   *
   * <p>If a delegate Terminal is configured, the transaction will be committed <em>after</em> the
   * delegate has dispatched the events.
   *
   * @param transacted whether dispatching should be transacted or not
   */
  public void setTransacted(boolean transacted) {
    isTransacted = transacted;
  }

  /**
   * Sets the TopicConnectionFactory providing the Connections to send messages on. The JmsPublisher
   * does not cache or reuse connections. Providing a ConnectionFactory instance that caches
   * connections will prevent new connections to be opened for each invocation to
   * {@link #send(List)}
   *
   * <p>Defaults to {@code java:/ConnectionFactory}.
   *
   * @param connectionFactoryName The name of the connection factory to use
   */
  public void setConnectionFactoryName(String connectionFactoryName) {
    this.connectionFactoryName = connectionFactoryName;
  }

  /**
   * Sets the TopicConnectionFactory providing the Connections to send messages on. The JmsPublisher
   * does not cache or reuse connections. Providing a ConnectionFactory instance that caches
   * connections will prevent new connections to be opened for each invocation to
   * {@link #send(List)}
   *
   * <p>Defaults to {@code java:/ConnectionFactory}.
   *
   * <p>This setting is ignored if a {@link #setConnectionFactory(TopicConnectionFactory)} is
   * configured.
   *
   * @param topicConnectionFactory The connection factory to set
   */
  public void setConnectionFactory(TopicConnectionFactory topicConnectionFactory) {
    this.connectionFactory = topicConnectionFactory;
  }

  /**
   * Sets the Message Converter that creates JMS Messages from Event Messages and vice versa.
   * Setting this property will ignore the "serializer" properties, which just act as short hands to
   * create a DefaultJmsMessageConverter instance.
   *
   * <p>Defaults to a DefaultJmsMessageConverter.
   *
   * @param messageConverter The message converter to convert AMQP Messages to Event Messages and
   *                         vice versa.
   */
  public void setMessageConverter(JmsMessageConverter messageConverter) {
    this.messageConverter = messageConverter;
  }

  /**
   * Whether or not messages should be marked as "persistent" when sending them out. Persistent
   * messages suffer from a performance penalty, but will survive a reboot of the Message broker
   * that stores them.
   *
   * <p>By default, messages are persistent.
   *
   * <p>Note that this setting is ignored if a {@link
   * #setMessageConverter(JmsMessageConverter) MessageConverter} is provided.
   * In that case, the message converter must add the properties to reflect the required
   * persistency setting.
   *
   * @param persistent whether or not messages should be persistent
   */
  public void setPersistent(boolean persistent) {
    isPersitent = persistent;
  }

  /**
   * Sets the serializer to serialize messages with when sending them to the Topic.
   *
   * <p>Defaults to an autowired serializer, which requires exactly 1 eligible serializer to be
   * present.
   *
   * <p>This setting is ignored if a {@link #setMessageConverter(JmsMessageConverter)
   * MessageConverter} is configured.
   *
   * @param serializer the serializer to serialize message with
   */
  public void setSerializer(Serializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Sets the name of the topic to dispatch published messages to.
   * Defaults to "{@code Axon.EventBus}".
   *
   * <p>This setting is ignored if a {@link #setTopic(Topic) Topic} is configured.
   *
   * @param topicName the name of the topic to dispatch messages to
   */
  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  /**
   * Sets the topic to dispatch published messages to. Defaults to the topic named
   * "{@code Axon.EventBus}".
   *
   * @param topic the topic to dispatch messages to
   */
  public void setTopic(Topic topic) {
    this.topic = topic;
  }

}
