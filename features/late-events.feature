Feature: Late GPS pings vs on-time
  Spec: docs/specs/spec-late-event-replay.md

  Scenario: a ping 2 minutes late is on-time; 20 minutes is late
    Given GPS pings arriving 2 minutes and 20 minutes after event_ts
    When the 10 minute watermark is applied
    Then exactly one ping is on-time and it is order a
    And exactly one ping is late and it is order b
