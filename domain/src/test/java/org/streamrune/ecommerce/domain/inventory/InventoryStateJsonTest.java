package org.streamrune.ecommerce.domain.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

/**
 * The bus snapshots an aggregate's state as JSON every few events. A state that read back without
 * its open reservations would make every later release and confirmation store nothing.
 */
class InventoryStateJsonTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void aRoundTripKeepsEveryOpenReservation() throws Exception {
    InventoryState state = new InventoryState("p-1", 90, 7, 3, Map.of("o-1", 3, "o-2", 4));

    InventoryState read = mapper.readValue(mapper.writeValueAsString(state), InventoryState.class);

    assertEquals(state, read);
    assertEquals(OptionalInt.of(3), read.reservationOf("o-1"));
    assertEquals(OptionalInt.of(4), read.reservationOf("o-2"));
    assertEquals(OptionalInt.empty(), read.reservationOf("o-3"));
  }

  @Test
  void aStateWrittenWithoutReservationsReadsNone() throws Exception {
    InventoryState read =
        mapper.readValue(
            "{\"productId\":\"p-1\",\"available\":5,\"reserved\":0,\"committed\":0}",
            InventoryState.class);

    assertTrue(read.reservations().isEmpty());
    assertEquals(5, read.available());
  }
}
