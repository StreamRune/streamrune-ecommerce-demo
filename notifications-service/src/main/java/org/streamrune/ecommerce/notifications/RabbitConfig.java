package org.streamrune.ecommerce.notifications;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

  @Bean
  public TopicExchange integrationExchange() {
    return new TopicExchange("streamrune.events", true, false);
  }

  @Bean
  public Queue integrationQueue(
      @Value("${app.integration.queue:streamrune.integration}") String queueName) {
    return new Queue(queueName, true);
  }

  @Bean
  public Binding integrationBinding(Queue integrationQueue, TopicExchange integrationExchange) {
    return BindingBuilder.bind(integrationQueue).to(integrationExchange).with("integration.#");
  }
}
