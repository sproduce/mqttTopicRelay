#include <sqlite3.h>
#include <mosquitto.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>


#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <syslog.h>


int globali = 10;

int sqlRc;

sqlite3 *db;
char *err_msg = 0;
char configPath[200];
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




struct mqtt_config{
    const char* host;
    int port;
    const char* login;
    const char* passwd;
} mqttConfig;



int getConfig(void *mConfig, int argc, char **argv, 
                    char **azColName) {
    struct mqtt_config *mConf = (struct mqtt_config *)mConfig;
    for (int i = 0; i < argc; i++){
        if (argv[i]){
            if (strcmp(azColName[i],"mqttHost") == 0){mConf->host = strdup(argv[i]);}
            if (strcmp(azColName[i],"mqttPort") == 0){mConf->port = atoi(argv[i]);}
            if (strcmp(azColName[i],"mqttLogin") == 0){mConf->login = strdup(argv[i]);}
            if (strcmp(azColName[i],"mqttPasswd") == 0){mConf->passwd = strdup(argv[i]);}
        }
        
    }



}



int getTopics(void *NotUsed, int argc, char **argv, 
                    char **azColName) {
    stopics.topics[stopics.count].name = strdup(argv[1]);
    stopics.topics[stopics.count].id = atoi(argv[0]);
    //printf("id = %i\nname = %s\n", stopics.topics[stopics.count].id, stopics.topics[stopics.count].name);
    stopics.count++;
    return 0;
}


void subscribeFromDb(struct mosquitto *mosq) {
    for (int i = 0; i < stopics.count; i++) {
        mosquitto_subscribe(mosq, NULL, stopics.topics[i].name , 1);
    }
}


int getSendTopic(void *mosq, int argc, char **argv, 
                    char **azColName) {
    struct mosquitto *msqt = (struct mosquitto *)mosq;    
    //stopics.topics[stopics.count].name = strdup(argv[1]);
    mosquitto_publish(msqt, NULL, argv[0], strlen(argv[1]), argv[1], 2, false);
    return 0;
}




void mConnect(struct mosquitto *mosq, void *obj, int rc)
{
    if (rc != MOSQ_ERR_SUCCESS){
        mosquitto_disconnect(mosq);
        //printf("Login Passwd ERROR\n");
        //printf("MQTT DISCONNECT ...... !!!\n");
    } else {
        //printf("Mosquitto connect ...... !!!\n");
        subscribeFromDb(mosq);
    }

}



void sendMessage(struct mosquitto *msq, unsigned int stopicId, int value) {
    sprintf(stringSql,"select topic, value from ptopic where stopic_id=%i and svalue=%i",stopicId, value);
    int sqlRc = sqlite3_exec(db, stringSql, getSendTopic, msq, &err_msg);

}



void mMessage(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg)
{
    for(int i = 0; i < stopics.count; i++) {
        if (!strcmp(stopics.topics[i].name, msg->topic)) {
            sendMessage(mosq, stopics.topics[i].id, atoi(msg->payload));
            //printf("Message: %i \n", stopics.topics[i].id);        
        }
    }
}




int newMain()
{
  int rc;



    sqlRc = sqlite3_open(configPath, &db);

    if (sqlRc != SQLITE_OK) {
        
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        
        return 1;
    }

    char *sqlConfig = "SELECT * FROM config";
    sqlRc = sqlite3_exec(db, sqlConfig, getConfig, &mqttConfig, &err_msg);




    mosquitto_lib_init();
    mosq = mosquitto_new(NULL, true, NULL);
    mosquitto_connect_callback_set(mosq, mConnect);
    mosquitto_message_callback_set(mosq, mMessage);

    rc = mosquitto_username_pw_set(mosq, mqttConfig.login, mqttConfig.passwd);
    rc = mosquitto_connect(mosq, mqttConfig.host, mqttConfig.port, 60);

 

    char *sql = "SELECT id,topic FROM stopic limit 99";
    sqlRc = sqlite3_exec(db, sql, getTopics, NULL, &err_msg);

    if (sqlRc != SQLITE_OK ) {

        fprintf(stderr, "Failed to select data\n");
        fprintf(stderr, "SQL error: %s\n", err_msg);

        sqlite3_free(err_msg);
        sqlite3_close(db);
        
        return 1;
    } 

    
 
    mosquitto_loop_start(mosq);
    //mosquitto_loop_forever(mosq, -1, 1);
    //printf("count topic = %i\n",stopics.count);
    for(;;) {
        //mosquitto_publish(mosq, NULL, "sinh/sinh", 1, "100", 2, false);
        //sleep(3);
    }


return 0;

}







static void start_daemon()
{
    pid_t pid;
    
    pid = fork();
    
    if (pid < 0)
        exit(EXIT_FAILURE);
    
    if (pid > 0)
        exit(EXIT_SUCCESS);
    
    /* On success: The child process becomes session leader */
    if (setsid() < 0)
        exit(EXIT_FAILURE);
    
    /* Catch, ignore and handle signals */
    /*TODO: Implement a working signal handler */
    signal(SIGCHLD, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    
    /* Fork off for the second time*/
    pid = fork();
    
    /* An error occurred */
    if (pid < 0)
        exit(EXIT_FAILURE);
    
    /* Success: Let the parent terminate */
    if (pid > 0)
        exit(EXIT_SUCCESS);
    
    /* Set new file permissions */
    umask(0);
    
    /* Change the working directory to the root directory */
    /* or another appropriated directory */
    chdir("/");
    
    /* Close all open file descriptors */
    int x;
    for (x = sysconf(_SC_OPEN_MAX); x>=0; x--)
    {
        close (x);
    }
    
    newMain();
}











int main(int argc, char* argv[]) {
    strcat(getcwd(configPath, 200),"/mqttTopic.db");
    printf("%s\n",configPath);
    start_daemon();

    return 0;
}



