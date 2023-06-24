package com.thecodinginterface.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer app writing single Order records at a time to a Kinesis Data Stream using
 * the PutRecord API of the JAVA SDK.
 *
 * The Seller ID field of each Order record is being used as the partition key which
 * groups orders sold by the same seller by shard and by order within their respective
 * shard. Ordering is also being overriden via explicit use of SequenceNumberForOrdering
 * for each PutRecord API request to guarantee increasing sequence number per partition key.
 */
public class PutRecordApp {
    static final Logger logger = LogManager.getLogger(PutRecordApp.class);
    static final ObjectMapper objMapper = new ObjectMapper();

    public static void main( String[] args ) {
        logger.info("Starting PutRecord Producer");
        String streamName = args[0];

        // Instantiate the client
        var client = KinesisClient.builder().build();

        // Add shutdown hook to cleanly close the client when program terminates
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down program");
            client.close();
        }, "producer-shutdown"));

        // map to maintain partition sequence numbers
        Map<String, String> partitionSequences = new HashMap<>();

        while (true) {
            // Generate fake order item data
            var order = OrderGenerator.makeOrder();
            logger.info(String.format("Generated %s", order));

            try {
                // construct single PutRecord request
                var partitionKey = order.getSellerID();
                var builder = PutRecordRequest.builder()
                                        .partitionKey(partitionKey)
                                        .streamName(streamName)
                                        .data(SdkBytes.fromByteArray(objMapper.writeValueAsBytes(order)));

                // if partition has a sequence number from a previous PutRecord request
                // use it to guarantee strict per partition ordering
                if (partitionSequences.containsKey(partitionKey)) {
                    builder = builder.sequenceNumberForOrdering(partitionSequences.get(partitionKey));
                }

                var putRequest = builder.build();

                // execute single PutRecord request and update partition sequence map
                PutRecordResponse response = client.putRecord(putRequest);
                partitionSequences.put(partitionKey, response.sequenceNumber());

                logger.info(String.format("Produced Record %s to Shard %s", response.sequenceNumber(), response.shardId()));
            } catch(JsonProcessingException e) {
                logger.error(String.format("Failed to serialize %s", order), e);
            } catch (KinesisException e) {
                logger.error(String.format("Failed to produce %s", order), e);
            }

            // introduce artificial delay for demonstration purpose / visual tracking of logging
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
