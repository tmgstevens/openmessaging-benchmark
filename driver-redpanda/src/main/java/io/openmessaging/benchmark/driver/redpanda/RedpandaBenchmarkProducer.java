/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.redpanda;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.openmessaging.benchmark.driver.BenchmarkProducer;

public class RedpandaBenchmarkProducer implements BenchmarkProducer {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final boolean useTransactions;
    private final Integer msgsPerTransaction;

    private boolean isTxInFlight = false;
    private int currentTxMessageCount = 0;

    public RedpandaBenchmarkProducer(KafkaProducer<String, byte[]> producer, String topic, Properties producerProperties) {
        this.producer = producer;
        this.topic = topic;
        this.useTransactions = Boolean.parseBoolean(producerProperties.getProperty("enableTransactions", "false"));
        this.msgsPerTransaction = Integer.valueOf(producerProperties.getProperty("msgsPerTransaction", "1"));
        producer.initTransactions();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key.orElse(null), payload);

        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            if (useTransactions && !isTxInFlight) {
                producer.beginTransaction();
                isTxInFlight = true;
                currentTxMessageCount++;
            }
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(null);
                }
            });
            if (useTransactions && currentTxMessageCount >= msgsPerTransaction) {
                producer.commitTransaction();
                isTxInFlight = false;
                future.complete(null);
            }
        } catch(Exception e) {
            try {
                if (useTransactions) {
                    producer.abortTransaction();
                    isTxInFlight = false;
                }
            } catch (Exception ex) {
                // No need to handle this, we're already throwing an exception at this point.
                // If the abort transaction failed, that's no better or worse than the previous exception
            } finally {
                future.completeExceptionally(e);
            }
        }

        return future;
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

}
