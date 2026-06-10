#include "qemu/osdep.h"
#include "net/pdes-engine.h"
#include "net/pdes-communicator.h"
#include "qemu/main-loop.h"
#include "sysemu/runstate.h"
#include "include/migration/snapshot.h"
#include "include/sysemu/runstate.h"
#include "net/pdes-checkpoint.h"
#include "migration/snapshot.h"
#include "sysemu/cpu-timers.h"
#include "hw/core/cpu.h"

#ifdef CONFIG_LIBQFLEX
#include "middleware/libqflex/libqflex-legacy-api.h"
#endif

// TODO this should be generlized to multiple neighbours later
// For now singleton pdes engine
extern PDESEngine *singleton_engine = NULL;

PDESEngine *get_singleton_engine(){
    return singleton_engine;
}


int64_t get_current_virtual_for_normal_message(PDESEngine *engine) {
    return get_universal_virtual_time(engine) + engine->latencyns;
}

int64_t get_current_virtual_for_destroy_message(PDESEngine *engine) {
    return get_universal_virtual_time(engine);
}

PDESEngine *pdes_engine_create(
    const char *shm_send,
    const char *shm_recv,
    bool sync,
    int64_t latencyns,
    PDESRecvCallback cb,
    void *opaque,
    PauseStatusCallBack pause_status_cb,
    void *pause_status_opaque,
    int64_t first_sync_virtual_time,
    bool master,
    bool phantom
) {
    // Show error if singleton was created before
    assert(singleton_engine == NULL && "Singleton engine already created");
    PDESEngine *engine = g_new0(PDESEngine, 1);
    engine->comm = pdes_comm_create(shm_send, shm_recv);
    engine->needs_sync = sync;
    engine->latencyns = latencyns;
    engine->recv_cb = cb;
    engine->recv_opaque = opaque;
    engine->has_first_sync = false;
    engine->sent_first_sync = false;
    engine->received_first_sync = false;
    engine->deferred_normal = g_queue_new();
    engine->waiting_for_quanta = false;
    engine->base_diff = 0;
    engine->paused = false;
    engine->pause_status_cb = pause_status_cb;
    engine->pause_status_opaque = pause_status_opaque;

    engine->first_sync_virtual_time = first_sync_virtual_time;
    engine->caclulated_time_diff = false;
    engine->base_time_diff = 0;

    engine->master = master;
    engine->init_flag = 0;
    engine->master_init = false;
    engine->pause_bh = NULL;
    engine->needs_to_checkpoint = false;
    engine->notified_neighbors = false;
    engine->boundry_checkpoint_bh = NULL;
    engine->skip_boundry_check_after_checkpoint = false;
    engine->phantom = phantom;
    // A phantom node produces no measurement output, so it's ready to exit from the start — seed
    // self_ready here. It then advertises CTRL_READY immediately and rides the master's CTRL_CLEANUP
    // (broadcast only after the MASTER finishes its own measurement/checkpoint), so the master never
    // blocks on it. Harmless in every phase: in init/FW the phantom has written its quantum-committed
    // checkpoint by the time CLEANUP arrives; in uniform timing its libphantomkraken keeps advancing
    // all cores at the estimated IPC right up to terminate, so it serves the full window first.
    engine->self_ready = phantom;
    engine->ready_peers = 0;
    engine->cleanup_sent = false;
    engine->cleanup_received = false;
    engine->ready_sent = false;
    engine->pending_ckp_init = false;
    engine->fw_exit_after_checkpoint = false;



    engine->msg_rec_poll_timer = timer_new_ns(QEMU_CLOCK_HOST, pdes_engine_poll, engine);
    // Schedule it IMMEDIATELY

    // TODO look into optimizing this
    timer_mod(engine->msg_rec_poll_timer, qemu_clock_get_ns(QEMU_CLOCK_HOST)+50000); // 5 microseconds

    int64_t current_time = qemu_clock_get_ns(QEMU_CLOCK_VIRTUAL);
    // TODO remove this field
    engine->first_sync_time = current_time;

    singleton_engine = engine;
    printf(">>>>>>> NET_INIT_PDES CALLED <<<<<<<\n");
    return engine;
}


// TODO clean this up, destroying needs clean up
void destroy_strategy(){
    PDESWWT *wwt_engine = get_singleton_wwt_engine();
    if (wwt_engine != NULL){
        // remove the sync timer
        if (wwt_engine->sync_check_timer != NULL){
            timer_free(wwt_engine->sync_check_timer);
            wwt_engine->sync_check_timer = NULL;
        }
    }
}
void pdes_engine_destroy(PDESEngine *engine) {
    // By the time we tear down, our exit signal (READY as a peer, or CLEANUP as the master) has
    // already gone out on a sync — nothing left to announce here.
    if(engine->needs_to_checkpoint){
        // Create bh
        if (!engine->boundry_checkpoint_bh){
            engine->boundry_checkpoint_bh = qemu_bh_new(create_checkpoint_bh, true);
        }
        qemu_bh_schedule(engine->boundry_checkpoint_bh);
    }else{
        destroy_strategy();
#ifdef CONFIG_LIBQFLEX
        libqflex_stop("Simulation terminated by flexus.");
#endif
        exit(0);
    }
}

int pdes_engine_send(PDESEngine *engine, Message *msg) {
    /* TODO: Add your PDES decision logic here */
    if (engine->first_sync_time == -1){
        if (msg->type == MSG_TYPE_SYNC){
            // TODO remove this field
            engine->first_sync_time = qemu_clock_get_ns(QEMU_CLOCK_VIRTUAL);
        } else {
            // Should not happen, as first message should be sync
            assert(false && "First message sent is not a sync message");
        }
    }
    return pdes_comm_send(engine->comm, msg);
}



void initiate_checkpoint_master(void *context){
    printf("Master initiating checkpoint after receiving initiation message from neighbor...\n");
    PDESEngine *engine = get_singleton_engine();
    if (!engine->master){
        assert(false && "Only master should receive checkpoint initiation callback");
    }
    printf("Master initiating systemic snapshot save for checkpoint initiation...\n");
    save_snapshot("init_warmed",
                true, NULL, false, NULL, SNAPSHOT_FORMAT_EXTERNAL_INCREMENTAL_BASE, NULL);
    qemu_bh_delete(engine->checkpoint_bh);
    printf("Master completed systemic snapshot save for checkpoint initiation.\n");

    return;
}
// Master-side: stage a master-initiated checkpoint of the given name for ALL peers. The name is a
// parameter (NOT hardcoded to init_warmed) so this is the general "master tells peers to checkpoint"
// entry point — send_sync then rides CTRL_CKP_REQUEST + this name on the next barrier and each peer
// adopts it and checkpoints at the agreed round.
void set_checkpoint_values_for_master(const char *snapshot_name){
    // if master is ready to initiate checkpoint start it
    PDESEngine *engine = get_singleton_engine();
    printf("Master is already initialized, initiating checkpoint immediately.\n");
    // TODO Make this repeated part into a function
    engine->notified_neighbors = false;
    engine->needs_to_checkpoint = true;
    engine->checkpoint_format = SNAPSHOT_FORMAT_EXTERNAL_INCREMENTAL_BASE;
    // Round is NOT chosen here: it is committed when send_sync rides the requires_checkpoint flag on
    // the next barrier message — the round of that sync is the round both nodes checkpoint at.
    snprintf(engine->checkpoint_name, sizeof(engine->checkpoint_name), "%s", snapshot_name);
    printf("Setting checkpoint values for master, snapshot name: %s, format: %d\n", engine->checkpoint_name, engine->checkpoint_format);
    // init_warmed is the last coordinated checkpoint of the init phase: flag it final so CTRL_CKP_FINAL
    // rides the request. Every node (master + peers) then joins the CTRL_READY/CTRL_CLEANUP exit
    // handshake after writing it, instead of the master destroying the engine and exiting while the
    // peers block in sync and get SIGKILLed. A phantom peer is already self_ready (seeded at creation).
    if (strcmp(snapshot_name, "init_warmed") == 0){
        engine->fw_exit_after_checkpoint = true;
    }
    // For now skipping
}
void process_message(PDESEngine *engine, Message *msg) {

    // Make sure the message goes up the chain before doing anything else
    engine->recv_cb(engine->recv_opaque, msg);


    // printf("PDES Engine received message of type %u with timestamp %lu ns and len %u bytes.\n", msg->type, msg->ts_ns, msg->len);

    // All control signals now ride the sync (CTRL_* bits in the byte at offset sizeof(round)); they are
    // therefore consumed at the quantum barrier when sync is on, and immediately (via the poll) when
    // off. The checkpoint REQUEST (CTRL_CKP_REQUEST) is handled in wwt_recivied_callback (recv_cb,
    // already run above); here we handle the exit handshake + checkpoint-init effects. PERMISSION is
    // processed before END so a single sync carrying both keeps the "permitted before END" invariant.
    if (msg->type == MSG_TYPE_SYNC){
        uint8_t ctrl = (msg->len > sizeof(uint64_t)) ? msg->data[sizeof(uint64_t)] : 0;
        if (ctrl & CTRL_CKP_INIT){
            engine->init_flag++;
            if (engine->master && engine->init_flag == 1 && engine->master_init){
                set_checkpoint_values_for_master("init_warmed");
            }
        }
        if ((ctrl & CTRL_READY) && engine->master){
            qatomic_inc(&engine->ready_peers);   // a peer's Flexus is ready to stop
        }
        if (ctrl & CTRL_CLEANUP){
            qatomic_set(&engine->cleanup_received, true);   // master says: terminate
        }
    }

}

void pdes_engine_poll(void *opaque) {
    PDESEngine *engine = opaque;

    while(true){
        Message msg;
        int res = pdes_comm_recv(engine->comm, &msg);
        if (res == NO_MESSAGE) {
            // No message to process
            break;
        } else if (res < 0 ) {
            // Error occurred while polling
            fprintf(stderr, "Error polling for messages: %d\n", res);
            break;
        } else {
            // Message received, process it
            process_message(engine, &msg);
        }
    }
    // TODO add a flag so that when calling this manually we don't reschedule again and again
    // schedule_poll(engine);
}

void schedule_poll(void *opaque){
    // getting rid of poll here
    printf("!!!!!!!!!!! should not be called TODO be removed !!!!!!!!!!!\n");
    PDESEngine *engine = opaque;
    timer_mod(engine->msg_rec_poll_timer, qemu_clock_get_ns(QEMU_CLOCK_HOST)+5000000);
}

/* Flag-only cooperative pause: never stop vCPU threads here (we're on a
 * virtual-time timer; vm_stop would freeze the recovery timer). PWQ-side
 * gates in dynamic_barrier.c (MTTCG) and tcg-accel-ops-rr.c (RR) read this. */
void pdes_pause(void *opaque){
    PDESEngine *engine = opaque;
    engine->paused = true;
#ifdef CONFIG_LIBQFLEX
    if (flexus_api.pause != NULL){
        flexus_api.pause();
    }else if(flexus_api.stop != NULL){
        assert(false && "Flexus resume API is not implemented, but stop API is implemented, this should not happen as both should be implemented together");
    }
#endif
}


void pdes_play(void *opaque){
    PDESEngine *engine = opaque;
#ifdef CONFIG_LIBQFLEX
    if (flexus_api.resume != NULL){
        flexus_api.resume();
    }else if(flexus_api.stop != NULL){
        assert(false && "Flexus resume API is not implemented, but stop API is implemented, this should not happen as both should be implemented together");
    }
#endif
    engine->paused = false;
}



// TODO: vestigial. With all messages delivered by the quantum barrier and checkpoints landing only at
// a barrier, nothing is ever in flight across a snapshot — pdes_inflight_add is disabled, so this only
// writes an empty JSON. Remove pdes_drain + its savevm.c calls + the whole in-flight machinery below.
int pdes_drain(PDESEngine *engine, char * snapshot_name, SnapshotFormat format) {
#ifdef CONFIG_LIBQFLEX
    assert(format == SNAPSHOT_FORMAT_EXTERNAL_INCREMENTAL_BASE && "qemu fork: pdes_drain only supports SNAPSHOT_FORMAT_EXTERNAL_INCREMENTAL_BASE");
#endif
    pdes_inflight_save_json(snapshot_name);
    return 0;
}




void finish_initiate_checkpoint(PDESEngine *engine){
    printf("========================GOT signal for initiate_checkpoint========================\n");
    // printf("+++++++++++++ Skipping checkpoint initiation because this is not implemented yet, just returning. +++++++++++++\n");
    // return;
    if (engine->master){
        // This is master, we can start checkpoint immediately
        // TODO expand this to multiple nodes
        engine->master_init = true;

        if (engine->init_flag >= 1){
            printf("Master received checkpoint initiation message, initiating checkpoint immediately.\n");
            // pdes_drain(engine, "init_warmed", SNAPSHOT_FORMAT_EXTERNAL_INCREMENTAL_BASE);

            // Make sure neighbors know they can checkpoint at end of quantum
            // and set flags for our selves as well
            set_checkpoint_values_for_master("init_warmed");
            printf("Master sent drain start message for checkpoint initiation to neighbors, waiting for neighbors to drain and checkpoint.\n");
        }else{
            printf("Master received checkpoint initiation message, but init flag is not set, marking master as ready and waiting for next checkpoint initiation message.\n");
            return;
        }
    }else{
        engine->pending_ckp_init = true;   // rides the next sync (CTRL_CKP_INIT)
        printf("Checkpoint init queued on sync for master.\n");
    }
}

bool can_stop(PDESEngine *engine){
    /* Runs on the Flexus thread. Record that this node's Flexus is ready to stop (send_sync reads this
     * on the main thread to emit CTRL_READY / CTRL_CLEANUP) and report whether we may terminate yet.
     * Never blocks: the master terminates once it has broadcast CLEANUP, a peer once it has received it.
     * qatomic because send_sync/process_message touch these on the main thread. */
    qatomic_set(&engine->self_ready, true);
    if (engine->master){
        return qatomic_read(&engine->cleanup_sent);
    }
    return qatomic_read(&engine->cleanup_received);
}