#include <glib.h>
#include <librdkafka/rdkafka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
struct stat st;

#include "common.c"

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


int main (int argc, char **argv) {
    rd_kafka_t *producer;
    rd_kafka_conf_t *conf;
    char errstr[512];
    const char *topic = "words";

    // Parse the command line.
    if (argc != 4) {
        printf("Para executar o programa você deve passar os arquivos getting_started.ini e o txt com as palavras como argumento\n");
        return 1;
    }

    // Parse the configuration.
    // See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    const char *config_file = argv[1];
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
    stat(argv[2], &st);
    int fileSize = st.st_size;
    char str[50];
    FILE* ptr;

    ptr = fopen(argv[2], "a+");

    int workersQtt = atoi(argv[3]);
     if (NULL == ptr) {
        printf("file can't be opened \n");
    }

    // Send file info to workers
    for (int i = 0; i < workersQtt; i++) {
        const char *key =  "abc";

        fgets(str, fileSize/workersQtt, ptr);
        size_t key_len = strlen(key);
        size_t value_len = strlen(str);
        char specific_topic[30];
        sprintf(specific_topic, "%s_%d", topic, i+1);
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

    g_message("%d events were produced to topic %s.", workersQtt, topic);

    rd_kafka_destroy(producer);
    return 0;
}