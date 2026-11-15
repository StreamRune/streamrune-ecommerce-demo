package org.streamrune.ecommerce.queries.paging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.ecommerce.queries.dto.OrderPage;

/** The page a request's {@code page} and {@code size} parameters name, and a page of a list. */
class ListPageTest {

  @Test
  void aRequestThatNamesNoPageGetsTheFirstPageOfTheDefaultSize() {
    assertEquals(new ListPage(0, 20), ListPage.parse(null, null));
    assertEquals(new ListPage(0, 20), ListPage.parse("", ""));
    assertEquals(new ListPage(3, 20), ListPage.parse("3", null));
    assertEquals(new ListPage(0, 5), ListPage.parse(null, "5"));
  }

  @Test
  void theSizeGoesFromOneToOneHundred() {
    assertEquals(1, ListPage.parse("0", "1").size());
    assertEquals(100, ListPage.parse("0", "100").size());
    assertRefused("0", "0", "size must be between 1 and 100");
    assertRefused("0", "101", "size must be between 1 and 100");
    assertRefused("0", "-1", "size must be between 1 and 100");
  }

  @Test
  void aNegativePageOrAValueThatIsNoWholeNumberIsRefused() {
    assertRefused("-1", null, "page must not be negative");
    assertRefused("one", null, "page must be a whole number");
    assertRefused("1.5", null, "page must be a whole number");
    assertRefused(null, "ten", "size must be a whole number");
    assertRefused(null, " 10", "size must be a whole number");
    assertRefused("99999999999", null, "page must be a whole number");
  }

  @Test
  void aPageWhoseFirstRowLiesPastTheLargestOffsetIsRefused() {
    assertRefused(
        String.valueOf(Integer.MAX_VALUE), "100", "page 2147483647 of size 100 is out of range");
    assertEquals(Integer.MAX_VALUE, ListPage.parse(String.valueOf(Integer.MAX_VALUE), "1").page());
  }

  @Test
  void theFrameworksPageRequestIsOrderedByTheRowIds() {
    assertEquals(PageRequest.of(2, 10, Sort.by("id")), new ListPage(2, 10).byId());
  }

  @Test
  void aSliceHoldsThePagesRowsAndTheWholeListsSize() {
    List<String> rows = List.of("a", "b", "c", "d", "e");

    Page<String> first = new ListPage(0, 2).slice(rows);
    assertEquals(List.of("a", "b"), first.content());
    assertEquals(5, first.totalElements());
    assertTrue(first.hasNext());

    Page<String> last = new ListPage(2, 2).slice(rows);
    assertEquals(List.of("e"), last.content());
    assertFalse(last.hasNext());

    Page<String> past = new ListPage(7, 2).slice(rows);
    assertTrue(past.content().isEmpty());
    assertEquals(5, past.totalElements());
    assertFalse(past.hasNext());

    Page<String> exact = new ListPage(1, 5).slice(rows);
    assertTrue(exact.content().isEmpty());
    assertFalse(new ListPage(0, 5).slice(rows).hasNext());
  }

  @Test
  void anOrderPageCarriesThePageAndWhetherAnotherFollows() {
    OrderPage page = OrderPage.of(new Page<>(List.of(), 41, 1, 20));
    assertEquals(1, page.page());
    assertEquals(20, page.size());
    assertEquals(41, page.total());
    assertTrue(page.hasNext());
    assertFalse(OrderPage.of(new Page<>(List.of(), 41, 2, 20)).hasNext());
  }

  private static void assertRefused(String page, String size, String message) {
    IllegalArgumentException refusal =
        assertThrows(IllegalArgumentException.class, () -> ListPage.parse(page, size));
    assertEquals(message, refusal.getMessage());
  }
}
