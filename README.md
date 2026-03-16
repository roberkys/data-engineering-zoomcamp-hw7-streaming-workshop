# Data Engineering Zoomcamp - Homework 7 Solutions

## streaming module

### Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it <redpanda-container> rpk version
```

**Answer:** `v25.3.9`

### Question 2. Sending data to Redpanda

How long did it take to send the data?
**Answer:** `10 seconds` (Actual: 7.22 seconds)

### Question 3. Consumer - trip distance

How many trips have `trip_distance` > 5?
**Answer:** `8506`

### Question 4. Tumbling window - pickup location

Which `PULocationID` had the most trips in a single 5-minute window?
**Answer:** `74`

### Question 5. Session window - longest streak

How many trips were in the longest session?
**Answer:** `81`

### Question 6. Tumbling window - largest tip

Which hour had the highest total tip amount?
**Answer:** `2025-10-16 18:00:00`
