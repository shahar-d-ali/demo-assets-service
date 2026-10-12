package com.demo.assetservice.events.producer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.assetservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventProducer {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public Transaction sendTransaction(Transaction transaction) {
        if (transaction == null) {
            throw new IllegalArgumentException("Transaction cannot be null");
        }
        sendTransactionEvent(List.of(transaction));
        return transaction;
    }

    public String sendTransactionEvent(List<Transaction> transactions) {
        if (transactions == null) {
            throw new IllegalArgumentException("Transactions cannot be null");
        }
        if (transactions.isEmpty()) {
            return "No transactions to send";
        }

        for (Transaction transaction : transactions) {
            String transactionId = transaction.getTransactionId().toString();
            String payload;
            try {
                payload = toJson(transaction);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Could not serialize transaction " + transactionId, e);
            }

            kafkaTemplate
                    .send(KafkaTopicConfig.TRANSACTION_TOPIC, transactionId, payload)
                    .whenComplete((result, exception) -> {
                        if (exception != null) {
                            log.error("Failed to send transaction {} to topic {}", transactionId,
                                    KafkaTopicConfig.TRANSACTION_TOPIC, exception);
                        } else {
                            log.info("Sent transaction {} to topic {}", transactionId,
                                    KafkaTopicConfig.TRANSACTION_TOPIC);
                        }
                    });
        }

        return "Submitted " + transactions.size() + " transactions to topic " + KafkaTopicConfig.TRANSACTION_TOPIC;
    }

    public List<Transaction> generateTransactions(int count) {
        int targetCount = count > 0 ? count : 100;
        List<Transaction> transactions = new ArrayList<>(targetCount);
        Random random = new Random();

        for (int i = 0; i < targetCount; i++) {
            // Generate amounts ranging up to 2000 so both normal and suspicious
            // transactions are generated
            double rawAmount = 10.0 + (random.nextDouble() * 1990.0);
            double amount = Math.round(rawAmount * 100.0) / 100.0;
            String type = random.nextBoolean() ? "DEBIT" : "CREDIT";

            Transaction transaction = new Transaction(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    amount,
                    "USD",
                    type,
                    "COMPLETED",
                    Instant.now());
            transactions.add(transaction);
        }
        return transactions;
    }

    private String toJson(Transaction transaction) throws JsonProcessingException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("transactionId", transaction.getTransactionId().toString());
        json.put("accountId", transaction.getAccountId() == null ? null : transaction.getAccountId().toString());
        json.put("amount", transaction.getAmount());
        json.put("currency", transaction.getCurrency() == null ? null : transaction.getCurrency().toString());
        json.put("type", transaction.getType() == null ? null : transaction.getType().toString());
        json.put("status", transaction.getStatus() == null ? null : transaction.getStatus().toString());
        json.put("timestamp", transaction.getTimestamp() == null ? null : transaction.getTimestamp().toString());
        return objectMapper.writeValueAsString(json);
    }
}
