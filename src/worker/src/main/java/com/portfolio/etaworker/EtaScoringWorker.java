package com.portfolio.etaworker;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * The one hot-path piece of this repo that runs as a Java/Spring Boot
 * worker: a stateless SQS consumer that computes a simple ETA heuristic
 * and writes only the CURRENT eta per order. It never touches history —
 * late-event correction happens only in the nightly PySpark replay
 * (src/replay.py). Redelivery-safe: writing the same order_id twice is a
 * plain overwrite, not a bug.
 */
@Component
@RestController
public class EtaScoringWorker {

    private static final String TABLE_NAME = "eta-current";
    private static final String QUEUE_NAME = "eta-scoring-queue";
    private static final double AVG_COURIER_SPEED_KMH = 22.0;
    private static final int PREP_TIME_MINUTES = 12;

    private final SqsClient sqsClient;
    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile long messagesProcessed = 0;
    private volatile long lastPollMillis = 0;

    public EtaScoringWorker(SqsClient sqsClient, DynamoDbClient dynamoDbClient) {
        this.sqsClient = sqsClient;
        this.dynamoDbClient = dynamoDbClient;
    }

    @Scheduled(fixedDelayString = "${worker.poll-interval-ms:1000}")
    public void pollAndScore() {
        String queueUrl;
        try {
            queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build()).queueUrl();
        } catch (Exception e) {
            return; // queue not bootstrapped yet — nothing to do
        }

        long start = System.currentTimeMillis();
        List<Message> messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(1)
                .build()).messages();

        for (Message message : messages) {
            try {
                scoreAndStore(message.body());
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
                messagesProcessed++;
            } catch (Exception e) {
                // leave the message in the queue; SQS redelivers it after the
                // visibility timeout, and a PutItem overwrite makes that safe.
            }
        }
        lastPollMillis = System.currentTimeMillis() - start;
    }

    private void scoreAndStore(String messageBody) throws Exception {
        JsonNode order = objectMapper.readTree(messageBody);
        String orderId = order.get("order_id").asText();
        double distanceKm = order.has("distance_km") ? order.get("distance_km").asDouble() : 4.0;

        double travelMinutes = (distanceKm / AVG_COURIER_SPEED_KMH) * 60.0;
        double etaMinutes = PREP_TIME_MINUTES + travelMinutes;

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("order_id", AttributeValue.builder().s(orderId).build());
        item.put("eta_minutes", AttributeValue.builder().n(String.valueOf(Math.round(etaMinutes * 10) / 10.0)).build());
        item.put("scored_at", AttributeValue.builder().s(Instant.now().toString()).build());

        dynamoDbClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item).build());
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "ok");
        status.put("messages_processed", messagesProcessed);
        status.put("last_poll_ms", lastPollMillis);
        return status;
    }
}
