package org.streamrune.ecommerce.micronaut.controller;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import org.streamrune.ecommerce.domain.inventory.InventoryCommand;
import org.streamrune.runtime.VirtualThreadCommandBus;

@Controller("/api/inventory")
public class InventoryCommandController {

  private final VirtualThreadCommandBus commandBus;

  public InventoryCommandController(VirtualThreadCommandBus commandBus) {
    this.commandBus = commandBus;
  }

  @Post("/{productId}/receive")
  public HttpResponse<Void> receiveShipment(
      @PathVariable String productId, @Body ReceiveShipmentRequest req) {
    commandBus.execute(new InventoryCommand.ReceiveShipment(productId, req.quantity()));
    return HttpResponse.ok();
  }

  @io.micronaut.serde.annotation.Serdeable
  public record ReceiveShipmentRequest(int quantity) {}
}
