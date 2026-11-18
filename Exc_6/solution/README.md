# Exercise 6 — Card Sentinel: write-up

## 1. Decision

I choose **log-based CDC with Debezium** (option 4). It is the only option that leaves the certified payment path completely untouched (R1): no code change, no extra queries on the system of record, and the card processor never waits for a fraud check. Because Debezium reads the write-ahead log, every committed payment arrives in Kafka in commit order, and rolled-back payments never appear, so no payment is missed or invented (R2). Polling would miss rows that commit out of `id` order and add query load for every agent; dual write changes the payment path and loses or invents events on a crash. The cost is the **replication slot**: PostgreSQL keeps all WAL the connector has not read yet, so if the connector is down for a weekend, the disk can fill up and stop the database and the payment path with it. In production I would monitor the WAL retained by the slot (`pg_replication_slots`, `pg_wal_lsn_diff`), the connector status and the consumer lag, alert well before the disk is full, and set a limit such as `max_slot_wal_keep_size`. If my team owned the payment path, I would choose the **transactional outbox**: the payment and its event are written in one transaction, and we control the event format ourselves.

## 2. The three runs

### Run 1 — Debezium's defaults
**Prediction:** the three velocity instances will not share the work. Debezium creates the topic with 1 partition, and one partition is read by only one instance per group, so vel-2 and vel-3 stay idle; vel-1 handles at most ~500 of the 1,000 payments/s, its lag grows and R4 fails. The key will be the primary key `{"id": ...}`.

```
R3  frauds caught          5/12  (42%)   FAIL  (>= 90 %)
R3  false alerts           0   PASS
R4  alert latency p50/p95  75.4 s / 124.1 s   FAIL  (p95 <= 5 s after the commit)
```
**Why:** the topic had only 1 partition (`partitions 1`, `members 3`), so only vel-1 got it (`assigned partitions [0]`), while vel-2 and vel-3 got `[]` and processed 0 transactions. vel-1 could not keep up with 1,000 payments/s, its lag grew from 4 s to 141 s, so alerts came minutes late (R4) and recent frauds were still in the backlog (R3). Partitions cap parallelism.

### Run 2 — 6 partitions
**Prediction:** all three instances will now get two partitions each and keep up, so R4 passes. But the key is still the payment `id`, so the payments of one card are spread over the partitions, and I expect the velocity model to miss most card-testing frauds.

```
R3  frauds caught          0/11  (0%)   FAIL  (>= 90 %)
R3  false alerts           0   PASS
R4  no alerts; worst lag of the velocity agents 6.9 s   FAIL  (<= 5 s)
```
**Why:** the partitions were shared (`[0, 1]`, `[2, 3]`, `[4, 5]`) and the lag dropped to about 1 s. But with key `{"id": ...}`, `hash(id) mod 6` spreads the 12 test payments of one stolen card over all six partitions, so each instance saw only about 4 of them, below the threshold of 8: no alert at all. R4 fails only because without alerts the scoreboard uses the worst lag (6.9 s, from the start-up peak).

### Run 3 — the cardholder as the key
**Prediction:** the key will be `{"user_id": ...}`. All payments of one cardholder go to the same partition and the same instance, so the velocity model catches its frauds (R3), and with 6 partitions R4 still passes.

```
R3  frauds caught          6/6  (100%)   PASS  (>= 90 %)
R3  false alerts           0   PASS
R4  alert latency p50/p95  1.1 s / 2.4 s   PASS  (p95 <= 5 s after the commit)
```
**Why:** with `message.key.columns = public.transactions:user_id`, every payment of a cardholder lands in the same partition and is read by one instance in commit order, so that instance sees the whole attack and flags the 8th payment. The work is still spread evenly (about 24,000 messages per partition), and the lag stayed around 1–2 s. Kafka UI confirms the key `{"user_id": 129077}`.

*Note:* in Run 1 and Run 2 the anomaly model's R4 also failed (p95 16.7 s and 42.7 s). ano-1 started with a backlog of 24–47 s, because the agents were started after the card processor, and needed a few minutes to catch up. In Run 3 its p95 was 2.2 s.

## 3. Closing answer

Debezium chose **1 partition** and the **primary key** as the message key. In production, the single partition would have capped the velocity model at one working instance: adding instances would change nothing, the lag would grow every second at peak load, and stolen cards would be blocked minutes or hours too late. The primary key as the key would have been worse because it fails silently: once the topic was scaled out, the payments of one card would be spread over instances, and the velocity model would catch almost no card-testing fraud, with no crash and no error message. The team could have noticed with **consumer lag monitoring** per group, by checking that **every instance has assigned partitions** (idle members), and above all with a **quality metric** like our scoreboard: a sudden drop in the number of velocity alerts, or in the detection rate on known or synthetic test frauds, compared with the number of confirmed chargebacks. Reviewing the topic configuration (partitions, key) as part of the data contract before going live would have caught both.

## 4. Final connector

See [`fraud-connector.json`](fraud-connector.json). Compared to the defaults it adds:

```json
"topic.creation.default.partitions": "6",
"topic.creation.default.replication.factor": "1",
"message.key.columns": "public.transactions:user_id",
```
