#include <glib.h>
#include <librdkafka/rdkafka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
struct stat st;

#define WORDS_QTT 1000

#include "common.c"
static volatile sig_atomic_t run = 1;

static void stop(int sig) {
    run = 0;
}
int results[3] = {0, 0, 0};
int interactions;
#define ARR_SIZE(arr) ( sizeof((arr)) / sizeof((arr[0])) )

/* Optional per-message delivery callback (triggered by poll() or flush())
 * when a message has been successfully delivered or permanently
 * failed delivery (after retries).
 */
static void dr_msg_cb (rd_kafka_t *kafka_handle,
                       const rd_kafka_message_t *rkmessage,
                       void *opaque) {
    if (rkmessage->err) {
        g_error("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    }
}

int send_message_to_broker(char *config_file, char *input_file){
    rd_kafka_t *producer;
    rd_kafka_conf_t *conf;
    char errstr[512];
    const char *topic = "words";
    // Parse the configuration.
    // See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
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


    // Get input file info
    stat(input_file, &st);
    int fileSize = st.st_size;
    char str[WORDS_QTT];
    FILE* ptr;

    ptr = fopen(input_file, "a+");
     if (NULL == ptr) {
        printf("file can't be opened \n");
    }
    interactions = fileSize/WORDS_QTT;
    // Send file info to workers
    for (int i = 0; i < interactions; i++) {
        char key[10];
        sprintf(key, "abc_%d", i);
        fgets(str, WORDS_QTT, ptr);
        size_t key_len = strlen(key);
        size_t value_len = strlen(str);
        char specific_topic[30];
        sprintf(specific_topic, "%s", topic);
        rd_kafka_resp_err_t err;

        err = rd_kafka_producev(producer,
                                RD_KAFKA_V_TOPIC(specific_topic),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_KEY((void*)key, key_len),
                                RD_KAFKA_V_VALUE((void*)str, value_len),
                                RD_KAFKA_V_OPAQUE(NULL),
                                RD_KAFKA_V_END);

        if (err) {
            g_error("Failed to produce to specific_topic %s: %s", specific_topic, rd_kafka_err2str(err));
            return 1;
        } else {
            g_message("Produced event to specific_topic %s: key = %s str = %s", specific_topic, key, str);
        }

        rd_kafka_poll(producer, 0);
    }
    fclose(ptr);


    // Block until the messages are all sent.
    g_message("Flushing final messages..");
    rd_kafka_flush(producer, 10 * 1000);

    if (rd_kafka_outq_len(producer) > 0) {
        g_error("%d message(s) were not delivered", rd_kafka_outq_len(producer));
    }

    g_message("%d events were produced to topic %s.", fileSize/WORDS_QTT, topic);

    rd_kafka_destroy(producer);
    return 0;
}

int consume_message_from_broker(char *config_file, char *topic, char *payload, int maxRecivedMessages, void (*f)(char*)){
    rd_kafka_t *consumer;
    char errstr[512];
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
            continue;
        }

        if (consumer_message->err) {
            if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            } else {
                g_message("Consumer error: %s", rd_kafka_message_errstr(consumer_message));
                return 1;
            }
        } else {
            sprintf(payload, "%s", (char *)consumer_message->payload);
            f(payload);
            recivedMessages++;
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

void received_message_callback(char* receivedMessage){
    int pos = 0;
    int initNumber, finishNumber;
    printf("Mensagem recebida: %s\n", receivedMessage);
    for(int i=0; i<strlen(receivedMessage); i++){
        if(receivedMessage[i] == ':'){
            initNumber = i+2;
        } else if(receivedMessage[i] == ','){
            char tmp[8];
            finishNumber = i-1;
            int size = finishNumber - initNumber + 1;
            memcpy(tmp, &receivedMessage[initNumber], size);
            tmp[size] = '\0';
            results[pos++]+= atoi(tmp);
        }
    }
}

int main (int argc, char **argv) {

    // Parse the command line.
    if (argc != 3) {
        printf("Para executar o programa você deve passar os arquivos getting_started.ini e o txt com as palavras como argumento\n");
        return 1;
    }
    send_message_to_broker(argv[1], argv[2]);
    char payload[512];
    consume_message_from_broker(argv[1], "answers", payload, interactions, received_message_callback);
    printf("O arquivo possui %d palavras e %d tem menos que 6 caracteres e %d possem 6 ou mais\n", results[0], results[1], results[2]);
    return 0;
}