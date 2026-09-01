Feature: Salting reduces restaurant_id partition skew
  Spec: docs/adr/0002-salt-skew.md

  Scenario: 900 hot-key rows vs 100 cold-key rows salt to a more balanced partition layout
    Given 900 rows for one hot restaurant_id and 100 rows spread across cold restaurant_ids
    When the rows are repartitioned naively by restaurant_id and again with a salted key
    Then the salted partition imbalance ratio is lower than the naive partition imbalance ratio
    And the total row count matches between the naive and salted approaches
