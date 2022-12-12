#include <glib.h>
#include <librdkafka/rdkafka.h>

#include <stdlib.h>
#include <string.h>

#include "common.c"

int consume_message_from_broker(char *config_file, char *topic, char *payload, int maxRecivedMessages){
    rd_kafka_t *consumer;
    rd_kafka_conf_t *conf;
    rd_kafka_resp_err_t err;
    int recivedMessages = 0;


    g_autoptr(GError) error = NULL;
    g_autoptr(GKeyFile) key_file = g_key_file_new();
    if (!g_key_file_load_from_file (key_file, config_file, G_KEY_FILE_NONE, &error)) {
        g_error ("Error loading config file: %s", error->message);
        return 1;
    }

    // Load the relevant configuration sections.
    conf = rd_kafka_conf_new();
    load_config_group(conf, key_file, "default");
    load_config_group(conf, key_file, "consumer");

    // Create the Consumer instance.
    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
        g_error("Failed to create new consumer: %s", errstr);
        return 1;
    }
    rd_kafka_poll_set_consumer(consumer);

    // Configuration object is now owned, and freed, by the rd_kafka_t instance.
    conf = NULL;

    rd_kafka_topic_partition_list_t *subscription = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(subscription, topic, RD_KAFKA_PARTITION_UA);

    // Subscribe to the list of topics.
    err = rd_kafka_subscribe(consumer, subscription);
    if (err) {
        g_error("Failed to subscribe to %d topics: %s", subscription->cnt, rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(consumer);
        return 1;
    }

    rd_kafka_topic_partition_list_destroy(subscription);

    // Install a signal handler for clean shutdown.
    signal(SIGINT, stop);

    // Start polling for messages.
    while (run && (maxRecivedMessages == -1 || recivedMessages < maxRecivedMessages)) {
        rd_kafka_message_t *consumer_message;

        consumer_message = rd_kafka_consumer_poll(consumer, 500);
        if (!consumer_message) {
            g_message("Waiting...");
            continue;
        }

        if (consumer_message->err) {
            if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                 */
            } else {
                g_message("Consumer error: %s", rd_kafka_message_errstr(consumer_message));
                return 1;
            }
        } else {
            payload = malloc(strlen((char *)consumer_message->payload)+1);
        }

        // Free the message when we're done.
        rd_kafka_message_destroy(consumer_message);
    }

    // Close the consumer: commit final offsets and leave the group.
    g_message( "Closing consumer");
    rd_kafka_consumer_close(consumer);

    // Destroy the consumer.
    rd_kafka_destroy(consumer);

    return 0;
}

int send_message_to_broker(char *config_file, char *topic, char *payload){
    rd_kafka_t *producer;
    rd_kafka_conf_t *conf;
    char errstr[512];

    config_file = argv[1];

    g_autoptr(GError) error = NULL;
    g_autoptr(GKeyFile) key_file = g_key_file_new();
    if (!g_key_file_load_from_file (key_file, config_file, G_KEY_FILE_NONE, &error)) {
        g_error ("Error loading config file: %s", error->message);
        return 1;
    }

    // Load the relevant configuration sections.
    conf = rd_kafka_conf_new();
    load_config_group(conf, key_file, "default");

    // Install a delivery-error callback.
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    // Create the Producer instance.
    producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        g_error("Failed to create new producer: %s", errstr);
        return 1;
    }

    // Configuration object is now owned, and freed, by the rd_kafka_t instance.
    conf = NULL;

    char key[64];
    sprintf(key, "%s_key", topic);
    size_t key_len = strlen(key);
    size_t value_len = strlen(payload);

    rd_kafka_resp_err_t err;

    err = rd_kafka_producev(producer,
                            RD_KAFKA_V_TOPIC(topic),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_KEY((void*)key, key_len),
                            RD_KAFKA_V_VALUE((void*)payload, value_len),
                            RD_KAFKA_V_OPAQUE(NULL),
                            RD_KAFKA_V_END);

    if (err) {
        g_error("Failed to produce to topic %s: %s", topic, rd_kafka_err2str(err));
        return 1;
    } else {
        g_message("Produced event to topic %s: key = %12s payload = %12s", topic, key, payload);
    }

    rd_kafka_poll(producer, 0);

    // Block until the messages are all sent.
    g_message("Flushing final messages..");
    rd_kafka_flush(producer, 10 * 1000);

    if (rd_kafka_outq_len(producer) > 0) {
        g_error("%d message(s) were not delivered", rd_kafka_outq_len(producer));
    }

    rd_kafka_destroy(producer);

    return 0;
}