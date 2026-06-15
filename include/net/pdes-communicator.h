#ifndef NET_PDES_COMMUNICATOR_H
#define NET_PDES_COMMUNICATOR_H

#include <stdint.h>
#include <stddef.h>

typedef struct PDESCommunicator PDESCommunicator;

#define MAX_MSG_SIZE 65536
#define RING_SIZE    8192

// Only two wire message types remain: NORMAL guest packets and SYNC (the quantum barrier message).
// All coordination (checkpoint, checkpoint-init, exit handshake) rides SYNC via the CTRL_* bits below,
// so it is delivered/processed together at the quantum barrier (or immediately when sync is off).
#define MSG_TYPE_NORMAL 0
#define MSG_TYPE_SYNC 1
#define NO_MESSAGE -1

// Control signals carried in the byte at offset sizeof(round) of a MSG_TYPE_SYNC.
#define CTRL_CKP_REQUEST  0x01   // master->peer: checkpoint at this sync's round (format+name follow)
#define CTRL_CKP_INIT     0x02   // peer->master: warmed and ready to checkpoint
#define CTRL_READY        0x04   // peer->master: this node's Flexus is ready to stop
#define CTRL_CLEANUP      0x08   // master->peer: everyone is ready, terminate
#define CTRL_CKP_FINAL    0x10   // master->peer: this checkpoint is the last FW snapshot; exit after writing it
#define CTRL_CKP_DONE     0x20   // peer->master: my coordinated checkpoint is on disk (save_snapshot returned)

typedef struct {
    uint64_t ts_ns;       /* timestamp in nanoseconds */
    uint32_t len;
    uint8_t  data[MAX_MSG_SIZE];
    uint8_t  type;        /* 0 = normal, 1 = sync TODO: define these properly */
} Message;



PDESCommunicator *pdes_comm_create(const char *shm_send_name, const char *shm_recv_name);
void pdes_comm_destroy(PDESCommunicator *comm);
Message create_message(const uint8_t *data, size_t len, uint8_t type, uint64_t ts_ns);
int pdes_comm_send(PDESCommunicator *comm, Message *msg);
int pdes_comm_recv(PDESCommunicator *comm, Message *msg);
/* Additive, log-only: cumulative NORMAL (guest data) bytes + message count moved over the wire. */
void pdes_comm_get_data_stats(uint64_t *bytes_sent, uint64_t *msgs_sent,
                              uint64_t *bytes_recv, uint64_t *msgs_recv);
/* 1 iff QFLEX_MEASURE_DATA_MOVEMENT is set — gates the data counters + [PDES-WIRE] report (off normally). */
int pdes_data_measure_enabled(void);

#endif