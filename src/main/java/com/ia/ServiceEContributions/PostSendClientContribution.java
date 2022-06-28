package com.ia.ServiceEContributions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class PostSendClientContribution {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(PostSendClientContribution.class.getName());

        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Set ACKS to 1 so only the leader replica needs to acknolwedge
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");

        //Set batch size to 32KB
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG,32768);

        //Create a Kafka producer from configuration
        KafkaProducer newContributionProducer = new KafkaProducer(kafkaProps);

        //Use a Random number to generate message keys
        Random randomKey = new Random();

        //Publish Asynchronously with Callback

        for(int i=0; i < 10; i++) {

            String messageKey = String.valueOf(randomKey.nextInt(1000));

            //Create Message
            logger.info("prepare message for topic topic_newcontrib, " + i);

            String msg = "{\"orderid\":\"" + messageKey + "\", \"custid\":\"" + i + "\", \"REER1\":\"value\", \"contributiontype\":\"1\", \"transdate\":\"2022/06/27\", \"amount\":\"10000.00\"}";
            logger.info("message value = " + msg);

            ProducerRecord<String, String> asyncRecCallBack =
                    new ProducerRecord<String, String>(
                            "topic_newcontrib",    //Topic name
                            messageKey,
                            msg
                    );

            //Send with Callback. Callback handler also has message key for context
            newContributionProducer.send(asyncRecCallBack, new UseCaseCallBack(messageKey));

            logger.info("\nSent Asynchronously with Callback :" + asyncRecCallBack);
            System.out.println("\nSent Asynchronously with Callback :" + asyncRecCallBack);

        }

        newContributionProducer.close();
    }
}


