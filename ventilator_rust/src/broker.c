#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <pthread.h> 

#include "zhelpers.h"

// -----------------------------------------------------------------------------
static void send_over (void *socket_from, void *socket_to)
{
    int rc;

    zmq_msg_t message;
    rc = zmq_msg_init (&message);
    assert (rc == 0);

    int size = zmq_msg_recv (&message, socket_from, 0);
    assert (size >= 0);

    if (zmq_msg_more (&message)) {
        rc = zmq_msg_send (&message, socket_to, ZMQ_SNDMORE);
        assert (rc != 0);
    } else {
        rc = zmq_msg_send (&message, socket_to, 0);
        assert (rc != 0);
    }

    rc = zmq_msg_close (&message);
    assert (rc == 0);
}

void *workersToClients(void *vargp) { 
    void *context = zmq_ctx_new ();
    int hwm = 1000000;

    //  PULL from workers
    void *puller = zmq_socket (context, ZMQ_PULL);
    zmq_setsockopt (puller, ZMQ_RCVHWM, &hwm, sizeof hwm);
    zmq_bind (puller, "tcp://*:5556");

    //  ROUTE to clients
    void *router = zmq_socket (context, ZMQ_ROUTER);
    zmq_setsockopt (router, ZMQ_SNDHWM, &hwm, sizeof hwm);
    zmq_bind (router, "tcp://*:5557");

    while (1) {
        send_over (puller, router);
    }

    zmq_close (puller);
    zmq_close (router);
    zmq_ctx_destroy (context);
    return NULL; 
} 

int main (void) {
    void *context = zmq_ctx_new ();
    int hwm = 1000000;

    //  PULL from client
    void *puller = zmq_socket (context, ZMQ_PULL);
    zmq_setsockopt (puller, ZMQ_RCVHWM, &hwm, sizeof hwm);
    zmq_bind (puller, "tcp://*:5554");

    //  PUSH to workers
    void *pusher = zmq_socket (context, ZMQ_PUSH);
    zmq_setsockopt (pusher, ZMQ_SNDHWM, &hwm, sizeof hwm);
    zmq_bind (pusher, "tcp://*:5555");

    // THREAD: workers -> clients
    pthread_t thread_id; 
    pthread_create(&thread_id, NULL, workersToClients, NULL); 

    while (1) {
        send_over (puller, pusher);
    }

    zmq_close (puller);
    zmq_close (pusher);
    zmq_ctx_destroy (context);
    return 0;
}
