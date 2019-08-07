#include <cstdio>
#include <stdint.h>
#define OFFSETOF(TYPE, ELEMENT) ((size_t)&(((TYPE *)0)->ELEMENT)) 

struct calvin_header {
    uint8_t node_id;
    uint8_t epoch_status;
    uint64_t epoch_id;
    uint64_t batch_size; // the batch size
    union {
        uint64_t chunk_size; // the number of calvin_requests in this rpc call
        volatile uint64_t received_size;
    };
} __attribute__ ((aligned (1)));
int main() {
    fprintf(stdout, "offset of = %d\n", OFFSETOF(calvin_header, node_id));
    fprintf(stdout, "offset of = %d\n", OFFSETOF(calvin_header, epoch_status));
    fprintf(stdout, "offset of = %d\n", OFFSETOF(calvin_header, epoch_id));
    fprintf(stdout, "offset of = %d\n", OFFSETOF(calvin_header, batch_size));
    fprintf(stdout, "offset of = %d\n", OFFSETOF(calvin_header, chunk_size));
    fprintf(stdout, "offset of = %d\n", OFFSETOF(calvin_header, received_size));
    fprintf(stdout, "sizeof calvin_header= %d\n", sizeof(calvin_header));
    return 0;
}
