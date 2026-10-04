package org.streamrune.ecommerce.notifications;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReceivedController {

  private final IntegrationEventListener listener;

  public ReceivedController(IntegrationEventListener listener) {
    this.listener = listener;
  }

  @GetMapping("/received")
  public List<IntegrationEventListener.Received> received() {
    return listener.recent();
  }
}
