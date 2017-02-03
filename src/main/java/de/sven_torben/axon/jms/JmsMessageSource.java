package de.sven_torben.axon.jms;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.axonframework.common.Registration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.messaging.SubscribableMessageSource;

public class JmsMessageSource implements MessageListener,
        SubscribableMessageSource<EventMessage<?>> {

  private final List<Consumer<List<? extends EventMessage<?>>>> eventProcessors
          = new CopyOnWriteArrayList<>();
  private JmsMessageConverter converter;

  /**
   * JMS Consumer and Converter to convert retrieve and convert Message.
   * @param consumer The consumer
   * @param converter The converter
   */
  public JmsMessageSource(MessageConsumer consumer, JmsMessageConverter converter) {
    try {
      this.converter = converter;
      consumer.setMessageListener(this);
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Registration subscribe(Consumer<List<? extends EventMessage<?>>> cnsmr) {
    eventProcessors.add(cnsmr);
    return () -> eventProcessors.remove(cnsmr);
  }

  @Override
  public void onMessage(Message msg) {
    try {
      converter.readJmsMessage(msg)
              .map(Collections::singletonList)
              .ifPresent(this::publish);
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void publish(List<? extends EventMessage<?>> events) {
    eventProcessors.forEach(p -> p.accept(events));
  }

}
