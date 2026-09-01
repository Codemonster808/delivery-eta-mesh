Feature: Worker redelivery does not duplicate scored orders
  Spec: tests/integration/test_worker_idempotency.py

  Scenario: redelivering the same order_id overwrites the existing eta-current row
    Given the eta scoring worker is running
    When an order is published and scored
    And the same order_id is redelivered
    Then there is exactly one eta-current row for that order_id
