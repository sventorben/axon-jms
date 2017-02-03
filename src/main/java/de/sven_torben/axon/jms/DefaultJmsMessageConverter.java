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

import java.time.Instant;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.axonframework.common.Assert;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.LazyDeserializingObject;
import org.axonframework.serialization.SerializedMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

/**
 * Default implementation of the JmsMessageConverter interface. This implementation will suffice in
 * most cases. It passes all meta-data entries as properties (with 'axon-metadata-' prefix) to the
 * message. Other message-specific attributes are also added as properties. The message payload is
 * serialized using the configured serializer and passed as the message body.
 *
 * @author Sven-Torben Janus
 */
public class DefaultJmsMessageConverter implements JmsMessageConverter {

  private final Serializer serializer;
  private final boolean persistent;

  /**
   * Initializes the JmsMessageConverter with the given {@code serializer} and requesting persistent
   * dispatching.
   *
   * @param serializer The serializer to serialize the Event Message's payload with
   */
  public DefaultJmsMessageConverter(Serializer serializer) {
    this(serializer, true);
  }

  /**
   * Initializes the JmsMessageConverter with the given {@code serializer} and requesting persistent
   * dispatching when {@code persistent} is {@code true}.
   *
   * @param serializer The serializer to serialize the Event Message's payload and Meta Data with
   * @param persistent Whether to request persistent message dispatching
   */
  public DefaultJmsMessageConverter(Serializer serializer, boolean persistent) {
    Assert.notNull(serializer, () -> "Serializer may not be null");
    this.serializer = serializer;
    this.persistent = persistent;
  }

  @Override
  public Message createJmsMessage(EventMessage<?> eventMessage, Session session)
          throws JMSException {

    SerializedObject<String> serializedObject
            = serializer.serialize(eventMessage.getPayload(), String.class);
    TextMessage jmsMessage = session.createTextMessage(serializedObject.getData());
    for (Map.Entry<String, Object> entry : eventMessage.getMetaData().entrySet()) {
      jmsMessage.setObjectProperty(
              "axon-metadata-" + entry.getKey(), entry.getValue());
    }
    jmsMessage.setObjectProperty("axon-message-id", eventMessage.getIdentifier());
    jmsMessage.setObjectProperty("axon-message-type", serializedObject.getType().getName());
    jmsMessage.setObjectProperty("axon-message-revision", serializedObject.getType().getRevision());
    jmsMessage.setObjectProperty("axon-message-timestamp", eventMessage.getTimestamp().toString());
    if (eventMessage instanceof DomainEventMessage) {
      jmsMessage.setObjectProperty("axon-message-aggregate-id",
              ((DomainEventMessage) eventMessage).getAggregateIdentifier());
      jmsMessage.setObjectProperty("axon-message-aggregate-seq",
              ((DomainEventMessage) eventMessage).getSequenceNumber());
      jmsMessage.setObjectProperty("axon-message-aggregate-type",
              ((DomainEventMessage) eventMessage).getType());
    }
    if (persistent) {
      jmsMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
    }
    return jmsMessage;
  }

  @Override
  public Optional<EventMessage<?>> readJmsMessage(Message msg) throws JMSException {
    if (!(msg instanceof TextMessage)) {
      return Optional.empty();
    }

    final TextMessage textMsg = (TextMessage) msg;

    if (textMsg.getObjectProperty("axon-message-id") == null
            || textMsg.getObjectProperty("axon-message-type") == null) {
      return Optional.empty();
    }
    Map<String, Object> metaData = new HashMap<>();
    Enumeration<String> propertyNames = textMsg.getPropertyNames();
    while (propertyNames.hasMoreElements()) {
      String propertyName = propertyNames.nextElement();
      if (propertyName.startsWith("axon-metadata-")) {
        metaData.put(propertyName.substring("axon-metadata-".length()),
                textMsg.getObjectProperty(propertyName));
      }
    }
    SimpleSerializedObject<String> serializedObject = new SimpleSerializedObject<>(
            textMsg.getText(), String.class,
            textMsg.getStringProperty("axon-message-type"),
            textMsg.getStringProperty("axon-message-revision"));
    SerializedMessage<EventMessage<?>> serializedMessage = new SerializedMessage<>(
            textMsg.getStringProperty("axon-message-id"),
            new LazyDeserializingObject<>(serializedObject, serializer),
            new LazyDeserializingObject<>(MetaData.from(metaData)));
    String timestamp = textMsg.getStringProperty("axon-message-timestamp");
    if (textMsg.propertyExists("axon-message-aggregate-id")) {
      return Optional.of(new GenericDomainEventMessage<>(
              textMsg.getStringProperty("axon-message-aggregate-type"),
              textMsg.getStringProperty("axon-message-aggregate-id"),
              textMsg.getLongProperty("axon-message-aggregate-seq"),
              serializedMessage,
          () -> Instant.parse(timestamp)));
    } else {
      return Optional.of(new GenericEventMessage<>(
              serializedMessage, () -> Instant.parse(timestamp)));
    }
  }
}
