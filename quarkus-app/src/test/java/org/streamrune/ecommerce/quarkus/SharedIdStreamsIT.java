package org.streamrune.ecommerce.quarkus;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/** A product and its inventory share an id value but never a stream. */
@QuarkusTest
@QuarkusTestResource(PostgresTestResource.class)
class SharedIdStreamsIT {

  @Test
  void productAndInventoryWithTheSameId_areTwoStreams() {
    String id = "p-shared-" + System.nanoTime();
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body(
            """
            {"productId": "%s", "name": "Shared", "description": "shared id", "category": "Test",
             "price": 1.00, "initialStock": 1}"""
                .formatted(id))
        .when()
        .post("/api/products")
        .then()
        .statusCode(200);
    given()
        .header("X-User-Role", "ADMIN")
        .header("X-User-Id", "test-admin")
        .contentType(ContentType.JSON)
        .body("{\"quantity\": 5}")
        .when()
        .post("/api/inventory/" + id + "/receive")
        .then()
        .statusCode(200);

    var product =
        given()
            .header("X-User-Role", "ADMIN")
            .when()
            .get("/api/events/product/" + id)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
    var inventory =
        given()
            .header("X-User-Role", "ADMIN")
            .when()
            .get("/api/events/inventory/" + id)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
    assertEquals(List.of("ProductCreated"), product.getList("eventType"));
    assertTrue(inventory.getList("eventType").contains("ShipmentReceived"));
    assertFalse(inventory.getList("eventType").contains("ProductCreated"));
    assertEquals("product:" + id, product.getString("[0].streamId"));
    assertEquals("inventory", inventory.getString("[0].aggregateType"));
    assertEquals(id, inventory.getString("[0].aggregateId"));
  }

  @Test
  void explorerRefusesANonConformingType_andMatchesNoSingleSegment() {
    given().header("X-User-Role", "ADMIN").when().get("/api/events/Order/x").then().statusCode(400);
    given().header("X-User-Role", "ADMIN").when().get("/api/events/o-1").then().statusCode(404);
    given()
        .header("X-User-Role", "ADMIN")
        .when()
        .get("/api/events/order/no-such-order")
        .then()
        .statusCode(200)
        .body("size()", Matchers.is(0));
  }
}
