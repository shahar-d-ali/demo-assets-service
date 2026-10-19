package com.demo.assetservice.events.producer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.demo.assetservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.Transaction;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TransactionEventProducer {

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    }

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TransactionEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

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

            kafkaTemplate
                    .send(KafkaTopicConfig.TRANSACTION_TOPIC, transactionId, transaction)
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
            // ~75% of transactions are suspicious (amount > 1000) to maximise fraud
            // detection signal; remaining 25% are normal (< 1000).
            double rawAmount;
            if (random.nextDouble() < 0.75) {
                // Suspicious: 1001–3000
                rawAmount = 1001.0 + (random.nextDouble() * 1999.0);
            } else {
                // Normal: 10–999
                rawAmount = 10.0 + (random.nextDouble() * 989.0);
            }
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
}
