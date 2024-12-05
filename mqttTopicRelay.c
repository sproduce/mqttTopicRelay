#include <sqlite3.h>
#include <mosquitto.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>



int globali = 10;

sqlite3 *db;
char *err_msg = 0;
struct mosquitto *mosq;


char stringSql[500];

typedef struct {
    char *name;
    unsigned int id;
}topic;

struct {
    topic topics[100];
	int count;
} stopics;



int getTopics(void *NotUsed, int argc, char **argv, 
                    char **azColName) {
    stopics.topics[stopics.count].name = strdup(argv[1]);
    stopics.topics[stopics.count].id = atoi(argv[0]);
    printf("id = %i\nname = %s\n", stopics.topics[stopics.count].id, stopics.topics[stopics.count].name);
    stopics.count++;
    return 0;
}


void subscribeFromDb(struct mosquitto *mosq) {
    for (int i = 0; i < stopics.count; i++) {
        mosquitto_subscribe(mosq, NULL, stopics.topics[i].name , 1);
    }
}


int getSendTopic(void *NotUsed, int argc, char **argv, 
                    char **azColName) {
    //stopics.topics[stopics.count].name = strdup(argv[1]);
        printf("Topic: %s, data: %i\n",argv[0],atoi(argv[1]));
        mosquitto_publish(mosq, NULL, argv[0], strlen(argv[1]), argv[1], 2, false);
    return 0;
}




void mConnect(struct mosquitto *mosq, void *obj, int rc)
{
    if (rc != MOSQ_ERR_SUCCESS){
        mosquitto_disconnect(mosq);
        printf("Login Passwd ERROR\n");
        printf("MQTT DISCONNECT ...... !!!\n");
    } else {
        printf("Mosquitto connect ...... !!!\n");
        subscribeFromDb(mosq);
        //mosquitto_subscribe(mosq, NULL, "test" , 1);
        subscribeFromDb(mosq);
    }

}



void sendMessage(unsigned int stopicId, int value) {
    printf("Topic Id: %i  Value: %i\n", stopicId, value);
    sprintf(stringSql,"select topic, value from ptopic where stopic_id=%i and svalue=%i",stopicId, value);
    int sqlRc = sqlite3_exec(db, stringSql, getSendTopic, 0, &err_msg);
    //printf("%s\n", stringSql);
}



void mMessage(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg)
{
    for(int i = 0; i < stopics.count; i++) {
        if (!strcmp(stopics.topics[i].name, msg->topic)) {
            sendMessage(stopics.topics[i].id, atoi(msg->payload));
            //printf("Message: %i \n", stopics.topics[i].id);        
        }
    }
}



int main(void) {

    int rc;
    mosquitto_lib_init();
    mosq = mosquitto_new(NULL, true, NULL);
    mosquitto_connect_callback_set(mosq, mConnect);
    mosquitto_message_callback_set(mosq, mMessage);

    rc = mosquitto_username_pw_set(mosq, "volodia", "123456");
    rc = mosquitto_connect(mosq, "test.mqtt.plainiot.com", 1883, 60);






    int sqlRc = sqlite3_open("./test.db", &db);

    if (sqlRc != SQLITE_OK) {
        
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        
        return 1;
    }

    char *sql = "SELECT id,topic FROM stopic limit 99";
    sqlRc = sqlite3_exec(db, sql, getTopics, NULL, &err_msg);

    if (sqlRc != SQLITE_OK ) {

        fprintf(stderr, "Failed to select data\n");
        fprintf(stderr, "SQL error: %s\n", err_msg);

        sqlite3_free(err_msg);
        sqlite3_close(db);
        
        return 1;
    } 

    
 //   printf("%s\n", sqlite3_libversion()); 
    mosquitto_loop_start(mosq);
    //mosquitto_loop_forever(mosq, -1, 1);
    printf("count topic = %i\n",stopics.count);
    for(;;) {
        //mosquitto_publish(mosq, NULL, "sinh/sinh", 1, "100", 2, false);
        //sleep(3);
    }


    return 0;
}



