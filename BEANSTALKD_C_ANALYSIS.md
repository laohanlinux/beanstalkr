# Deep Analysis of Beanstalkd C Source Code

This document provides a detailed analysis of the beanstalkd C implementation, focusing on commonly missed implementation details that are crucial for a correct Rust port.

## Table of Contents
1. [Job Hash Table (Prime-based Sizing & Rehashing)](#1-job-hash-table-prime-based-sizing--rehashing)
2. [Waiting Connections List (Round-Robin Fairness)](#2-waiting-connections-list-round-robin-fairness)
3. [Tube Pause Implementation](#3-tube-pause-implementation)
4. [WAL Reservation System](#4-wal-reservation-system)
5. [Epoll Queue Batching](#5-epoll-queue-batching)
6. [DEADLINE_SOON Logic](#6-deadline_soon-logic)
7. [Job State Transitions](#7-job-state-transitions)
8. [Buried Jobs Tracking](#8-buried-jobs-tracking)
9. [Delayed Jobs Processing](#9-delayed-jobs-processing)
10. [Potential Missing Features in Rust Port](#10-potential-missing-features-in-rust-port)

---

## 1. Job Hash Table (Prime-based Sizing & Rehashing)

### Location: `job.c`, `primes.c`

The job hash table uses a prime-based sizing strategy with specific load factors for upscaling and downscaling.

### Key Implementation Details:

```c
// From primes.c - 48 primes on 64-bit, 19 on 32-bit
size_t primes[] = {
    12289,    // NA / 3072 (upscale threshold)
    24593,    // 1537 / 6148
    49193,    // 3074 / 12298
    // ... continues
};

// From job.c
static Job *all_jobs_init[12289] = {0};  // Initial bucket array on stack
static Job **all_jobs = all_jobs_init;   // Pointer to current buckets
static size_t all_jobs_cap = 12289;      // Current capacity
static size_t all_jobs_used = 0;         // Number of jobs stored
static int cur_prime = 0;                // Current index in primes array
```

### Hash Function:
```c
static int _get_job_hash_index(uint64 job_id) {
    return job_id % all_jobs_cap;  // Simple modulo with prime
}
```

### Upscaling (Load Factor > 4):
```c
// Trigger: all_jobs_used > (all_jobs_cap << 2)  [i.e., > 4x capacity]
static void store_job(Job *j) {
    // ... store job ...
    if (all_jobs_used > (all_jobs_cap << 2)) rehash(1);
}
```

### Downscaling (Load Factor < 1/16):
```c
// Trigger: all_jobs_used < (all_jobs_cap >> 4)  [i.e., < 1/16 capacity]
static void job_hash_free(Job *j) {
    // ... remove job ...
    if (all_jobs_used < (all_jobs_cap >> 4)) rehash(0);
}
```

### Rehash Implementation:
```c
static void rehash(int is_upscaling) {
    Job **old = all_jobs;
    size_t old_cap = all_jobs_cap;
    int d = is_upscaling ? 1 : -1;

    if (cur_prime + d >= NUM_PRIMES) return;
    if (cur_prime + d < 0) return;
    if (is_upscaling && hash_table_was_oom) return;

    cur_prime += d;
    all_jobs_cap = primes[cur_prime];
    all_jobs = calloc(all_jobs_cap, sizeof(Job *));
    
    // Rehash all entries
    for (i = 0; i < old_cap; i++) {
        while (old[i]) {
            Job *j = old[i];
            old[i] = j->ht_next;
            j->ht_next = NULL;
            store_job(j);  // Re-insert
        }
    }
}
```

### ⚠️ Critical Details Often Missed:
1. **Initial array on stack**: The initial `all_jobs_init[12289]` is stack-allocated, not heap
2. **OOM handling**: `hash_table_was_oom` flag prevents repeated allocation attempts
3. **Separate chaining**: Uses singly-linked list via `Job.ht_next`
4. **No tombstones**: Jobs are physically removed from hash chains
5. **64-bit vs 32-bit**: Different number of primes available

---

## 2. Waiting Connections List (Round-Robin Fairness)

### Location: `ms.c` (Multiset)

The waiting connections list uses a custom "Multiset" data structure with round-robin fairness.

### Key Implementation:

```c
struct Ms {
    size_t len;
    size_t cap;
    size_t last;        // <-- KEY: Position of last taken element
    void **items;
    ms_event_fn oninsert;
    ms_event_fn onremove;
};
```

### Round-Robin Algorithm (ms_take):
```c
void *ms_take(Ms *a) {
    void *item;

    if (!a->len) return NULL;

    // Round-robin: advance last position
    a->last = a->last % a->len;
    item = a->items[a->last];
    ms_delete(a, a->last);  // Removes and shifts elements
    ++a->last;              // Next position for next take
    return item;
}
```

### Delete (with swap-to-end):
```c
static int ms_delete(Ms *a, size_t i) {
    void *item = a->items[i];
    // Swap with last element (not shift - O(1))
    a->items[i] = a->items[--a->len];
    
    if (a->onremove)
        a->onremove(a, item, i);
    return 1;
}
```

### ⚠️ Critical Details:
1. **`last` persists across calls**: The position is remembered between takes
2. **Swap, not shift**: Removal is O(1) via swap with last element
3. **Fairness guarantee**: Oldest waiting connection gets the next job
4. **Event callbacks**: `oninsert`/`onremove` hooks for reference counting

### Tube's waiting_conns:
```c
struct Tube {
    Ms waiting_conns;  // Connections waiting for jobs in this tube
    // ...
};
```

Each tube maintains its own waiting connections list. When a job is ready, `ms_take` is called to get the next waiting connection in round-robin order.

---

## 3. Tube Pause Implementation

### Location: `prot.c`, `dat.h`

Tube pause affects reservation timing but NOT job enqueueing.

### Data Structure:
```c
struct Tube {
    // pause is set to the duration of the current pause, otherwise 0, in nsec.
    int64 pause;
    
    // unpause_at is a timestamp when to unpause the tube, in nsec.
    int64 unpause_at;
    // ...
};
```

### Pause Command (pause-tube):
```c
case OP_PAUSE_TUBE:
    // ... parse arguments ...
    
    // Always pause for a positive amount of time, to make sure
    // that waiting clients wake up when the deadline arrives.
    if (delay == 0) {
        delay = 1;  // <-- KEY: Minimum 1ns pause
    }

    t->unpause_at = nanoseconds() + delay;
    t->pause = delay;
    t->stat.pause_ct++;
```

### Pause Check During Reservation:
```c
// From next_awaited_job() in prot.c
static Job *next_awaited_job(int64 now) {
    for (i = 0; i < tubes.len; i++) {
        Tube *t = tubes.items[i];
        if (t->pause) {
            if (t->unpause_at > now)
                continue;  // Skip this tube - it's paused
            t->pause = 0;  // Auto-unpause if time expired
        }
        if (t->waiting_conns.len && t->ready.len) {
            // Can reserve from this tube
        }
    }
}
```

### Unpause in prottick:
```c
int64 prottick(Server *s) {
    // ...
    for (i = 0; i < tubes.len; i++) {
        t = tubes.items[i];
        d = t->unpause_at - now;
        if (t->pause && d <= 0) {
            t->pause = 0;
            process_queue();  // Try to process waiting connections
        }
        else if (d > 0) {
            period = min(period, d);  // Schedule next tick
        }
    }
}
```

### ⚠️ Critical Details:
1. **Pause only affects reservations**: Jobs can still be added to paused tubes
2. **Minimum 1ns pause**: Zero is converted to 1 nanosecond
3. **Auto-unpause**: Checked in both `next_awaited_job` and `prottick`
4. **Wakes up waiters**: `process_queue()` called on unpause

---

## 4. WAL Reservation System

### Location: `walg.c`, `file.c`

The WAL uses a sophisticated reservation system to ensure disk space is available before writing.

### Key Concepts:
- **Reservation**: Space reserved in advance for future writes
- **Balance invariants**: Constraints on how reservations are distributed
- **Compaction**: Moving jobs from old files to reclaim space

### Reservation Types:

```c
// Reserve space for a new job (full record + delete)
int walresvput(Wal *w, Job *j) {
    int z = 0;
    z += sizeof(int);           // tube name length
    z += strlen(j->tube->name); // tube name
    z += sizeof(Jobrec);        // job record
    z += j->r.body_size;        // job body
    z += sizeof(int);           // delete record: name len (0)
    z += sizeof(Jobrec);        // delete record
    return reserve(w, z);
}

// Reserve space for an update (e.g., release with delay)
int walresvupdate(Wal *w) {
    int z = 0;
    z += sizeof(int);      // name len (0 for short record)
    z += sizeof(Jobrec);   // job record only
    return reserve(w, z);
}
```

### Core Reservation Algorithm:
```c
static int reserve(Wal *w, int n) {
    if (!w->use) return 1;

    // Fast path: space in current file
    if (w->cur->free >= n) {
        w->cur->free -= n;
        w->cur->resv += n;
        w->resv += n;
        return n;
    }

    // Need new file
    r = needfree(w, n);
    if (r != n) return 0;  // Failed

    w->tail->free -= n;
    w->tail->resv += n;
    w->resv += n;
    
    // Critical: Maintain balance invariants
    if (!balance(w, n)) {
        // Undo on failure
        w->resv -= n;
        w->tail->resv -= n;
        w->tail->free += n;
        return 0;
    }
    return n;
}
```

### Balance Invariants:
```c
// z = size of delete record
static const int z = sizeof(int) + sizeof(Jobrec);

// Ensures:
// 1. w->cur->resv >= n
// 2. w->cur->resv ≡ n (mod z)
// 3. Future files have resv ≡ 0 (mod z)
static int balance(Wal *w, int n) {
    // Move reservations from cur to tail if needed
    while (w->cur->resv < n) {
        int m = w->cur->resv;
        moveresv(w->tail, w->cur, m);
        usenext(w);  // Advance to next file
    }
    
    // Fix congruence via balancerest
    return balancerest(w, w->cur, n);
}
```

### File Structure:
```c
struct File {
    int free;    // Unreserved free bytes
    int resv;    // Reserved bytes (not yet written)
    Job jlist;   // Jobs written in this file
    // ...
};

struct Wal {
    int64 resv;  // Total reserved across all files
    int64 alive; // Bytes actually in use
    File *cur;   // Current file for writing
    File *tail;  // Last file (for reservations)
    // ...
};
```

### Compaction (walcompact):
```c
static int ratio(Wal *w) {
    int64 d = w->alive + w->resv;
    int64 n = (int64)w->nfile * w->filesize - d;
    if (!d) return 0;
    return n / d;  // Ratio of wasted space to used space
}

static void walcompact(Wal *w) {
    // Trigger: ratio >= 2 (50% waste)
    for (r = ratio(w); r >= 2; r--) {
        moveone(w);  // Move job from oldest to newest file
    }
}
```

### ⚠️ Critical Details:
1. **Reservation tracking**: Every write consumes reserved space
2. **Congruence invariant**: Reservations aligned to delete record size
3. **Migration**: `moveone()` rewrites jobs to compact files
4. **Garbage collection**: `walgc()` unlinks files with zero refs
5. **Two-phase write**: Reserve first, then write via `walwrite()`

---

## 5. Epoll Queue Batching
n
### Location: `prot.c` (epollq_* functions)

The epoll queue batches socket interest changes to minimize syscalls.

### Data Structure:
```c
// Single linked list with connections needing epoll updates
static Conn *epollq;
```

### Adding to Queue:
```c
static void epollq_add(Conn *c, char rw) {
    c->rw = rw;
    connsched(c);  // Schedule in timeout heap
    
    // Push to front of list (O(1))
    c->next = epollq;
    epollq = c;
}
```

### Removing from Queue:
```c
static void epollq_rmconn(Conn *c) {
    Conn *x, *newhead = NULL;

    // Rebuild list without c (O(n))
    while (epollq) {
        x = epollq;
        epollq = epollq->next;
        x->next = NULL;

        if (x != c) {
            x->next = newhead;
            newhead = x;
        }
    }
    epollq = newhead;
}
```

### Applying Changes:
```c
static void epollq_apply() {
    Conn *c;

    while (epollq) {
        c = epollq;
        epollq = epollq->next;
        c->next = NULL;
        
        int r = sockwant(&c->sock, c->rw);  // epoll_ctl
        if (r == -1) {
            connclose(c);
        }
    }
}
```

### Integration Points:
```c
// In h_accept - after handling new connection
epollq_apply();

// In h_conn - after processing I/O
if (c->state == STATE_CLOSE) {
    epollq_rmconn(c);
    connclose(c);
}
epollq_apply();

// In prottick - at end of tick
epollq_apply();
```

### ⚠️ Critical Details:
1. **Batching**: Multiple changes applied in single epollq_apply() call
2. **O(n) removal**: Removing specific conn requires list traversal
3. **Null on apply**: List is consumed (NULLed) during apply
4. **Next pointer reuse**: Conn.next used for both epollq and other lists

---

## 6. DEADLINE_SOON Logic

### Location: `conn.c`, `prot.c`

DEADLINE_SOON is returned when a reserved job's TTR is about to expire.

### Safety Margin:
```c
#define SAFETY_MARGIN (1000000000) /* 1 second in nanoseconds */
```

### Detection (conndeadlinesoon):
```c
int conndeadlinesoon(Conn *c) {
    int64 t = nanoseconds();
    Job *j = connsoonestjob(c);

    return j && t >= j->r.deadline_at - SAFETY_MARGIN;
}
```

### Getting Soonest Job (connsoonestjob):
```c
Job *connsoonestjob(Conn *c) {
    // Use cached value
    if (c->soonest_job != NULL)
        return c->soonest_job;

    Job *j = NULL;
    for (j = c->reserved_jobs.next; j != &c->reserved_jobs; j = j->next) {
        conn_set_soonestjob(c, j);
    }
    return c->soonest_job;
}

static void conn_set_soonestjob(Conn *c, Job *j) {
    if (!c->soonest_job || j->r.deadline_at < c->soonest_job->r.deadline_at) {
        c->soonest_job = j;
    }
}
```

### Reserve Handling:
```c
case OP_RESERVE:
    // Check BEFORE waiting
    if (conndeadlinesoon(c) && !conn_ready(c)) {
        reply_msg(c, MSG_DEADLINE_SOON);
        return;
    }
    wait_for_job(c, timeout);
    process_queue();
```

### Timeout Handling (conn_timeout):
```c
static void conn_timeout(Conn *c) {
    int should_timeout = 0;

    // Check if the client was trying to reserve a job
    if (conn_waiting(c) && conndeadlinesoon(c))
        should_timeout = 1;

    // Process expired reserved jobs
    while ((j = connsoonestjob(c))) {
        if (j->r.deadline_at >= nanoseconds())
            break;  // Not expired yet

        // Handle timeout - return to ready queue
        timeout_ct++;
        j->r.timeout_ct++;
        int r = enqueue_job(c->srv, remove_this_reserved_job(c, j), 0, 0);
        if (r < 1) bury_job(c->srv, j, 0);
        connsched(c);
    }

    if (should_timeout) {
        remove_waiting_conn(c);
        reply_msg(c, MSG_DEADLINE_SOON);
    } else if (conn_waiting(c) && c->pending_timeout >= 0) {
        // Regular timeout
        c->pending_timeout = -1;
        remove_waiting_conn(c);
        reply_msg(c, MSG_TIMED_OUT);
    }
}
```

### Connection Scheduling (connsched):
```c
void connsched(Conn *c) {
    // Remove from heap if present
    if (c->in_conns) {
        heapremove(&c->srv->conns, c->tickpos);
        c->in_conns = 0;
    }
    
    // Calculate next tick time
    c->tickat = conntickat(c);
    if (c->tickat) {
        heapinsert(&c->srv->conns, c);
        c->in_conns = 1;
    }
}

static int64 conntickat(Conn *c) {
    int margin = 0, should_timeout = 0;
    int64 t = INT64_MAX;

    if (conn_waiting(c)) {
        margin = SAFETY_MARGIN;  // Wake early if waiting
    }

    if (has_reserved_job(c)) {
        t = connsoonestjob(c)->r.deadline_at - nanoseconds() - margin;
        should_timeout = 1;
    }
    if (c->pending_timeout >= 0) {
        t = min(t, ((int64)c->pending_timeout) * 1000000000);
        should_timeout = 1;
    }

    if (should_timeout) {
        return nanoseconds() + t;
    }
    return 0;
}
```

### ⚠️ Critical Details:
1. **1-second safety margin**: Job considered "soon" when < 1s remains
2. **Cached soonest_job**: Avoids O(n) scan on every check
3. **Invalidated on change**: soonest_job cleared when reserved jobs change
4. **Pre-reserve check**: Checked BEFORE entering wait state
5. **During wait**: Connection woken early (with margin) for deadline

---

## 7. Job State Transitions

### Location: `prot.c`, `job.c`

### States (from dat.h):
```c
enum {
    Invalid,   // Job deleted
    Ready,     // In ready heap, available for reservation
    Reserved,  // Reserved by a connection
    Buried,    // In buried linked list
    Delayed,   // In delay heap, waiting for deadline
    Copy       // Temporary copy (for peek/stats)
};
```

### State Transition Diagram:

```
                    +---------+
                    |  PUT    |
                    +----+----+
                         |
                         v
    +--------------+  +--+------+  +--------------+
    |   release    |  | Delayed |  |   reserve    |
    |   (delay>0)  +<-+         +->+   (immed)    |
    +------+-------+  +----+----+  +------+-------+
           |               |              |
           v               | delay        v
    +------+-------+       | expired +----+----+
    |              |       |         |         |
    v              |       v         v         |
+---+---+    +-----+--+  +-+--+   +--+---+     |
|Buried |<---+ Ready  +->+    |   |Reserved|    |
+---+---+    +----+---+  +----+   +--+-----+    |
   | ^             |                  |         |
   | |   kick      |      timeout     |         |
   | +-------------+------------------+         |
   |                                            |
   +--------------------------------------------+
                  bury (from reserved)

+---------+     delete      +---------+
|  Any    +---------------->+ Invalid |
+---------+                 +---------+
```

### Key Transition Functions:

```c
// Enqueue job (handles Ready vs Delayed)
static int enqueue_job(Server *s, Job *j, int64 delay, char update_store) {
    j->reserver = NULL;
    if (delay) {
        j->r.deadline_at = nanoseconds() + delay;
        heapinsert(&j->tube->delay, j);
        j->r.state = Delayed;
    } else {
        heapinsert(&j->tube->ready, j);
        j->r.state = Ready;
        ready_ct++;
        // ... urgent count ...
    }
    // ... WAL ...
    process_queue();
}

// Reserve job
void conn_reserve_job(Conn *c, Job *j) {
    j->tube->stat.reserved_ct++;
    j->r.reserve_ct++;
    j->r.deadline_at = nanoseconds() + j->r.ttr;
    j->r.state = Reserved;
    job_list_insert(&c->reserved_jobs, j);
    j->reserver = c;
    c->pending_timeout = -1;
    conn_set_soonestjob(c, j);
}

// Bury job
static int bury_job(Server *s, Job *j, char update_store) {
    job_list_insert(&j->tube->buried, j);
    global_stat.buried_ct++;
    j->tube->stat.buried_ct++;
    j->r.state = Buried;
    j->reserver = NULL;
    j->r.bury_ct++;
    // ... WAL ...
}
```

### ⚠️ Critical Details:
1. **reserver pointer**: Set on reserve, cleared on state change
2. **deadline_at**: Different meanings per state (TTR for Reserved, delay for Delayed)
3. **process_queue()**: Called after enqueue to immediately match with waiters
4. **WAL sync**: State changes written to WAL before confirming to client

---

## 8. Buried Jobs Tracking

### Location: `tube.c`, `prot.c`

Buried jobs are stored in a **circular doubly-linked list** per tube.

### Data Structure:
```c
struct Tube {
    Job buried;  // <-- List HEADER (not a real job)
};
```

### List Initialization:
```c
Tube *make_tube(const char *name) {
    // ...
    Job j = {.tube = NULL};
    t->buried = j;
    t->buried.prev = t->buried.next = &t->buried;  // Point to self
    // ...
}
```

### List Operations:
```c
// Reset (make head point to itself)
void job_list_reset(Job *head) {
    head->prev = head;
    head->next = head;
}

// Check if empty
int job_list_is_empty(Job *head) {
    return head->next == head && head->prev == head;
}

// Insert at tail (before head)
void job_list_insert(Job *head, Job *j) {
    if (!job_list_is_empty(j)) return;  // Already in list

    j->prev = head->prev;
    j->next = head;
    head->prev->next = j;
    head->prev = j;
}

// Remove from list
Job *job_list_remove(Job *j) {
    if (!j) return NULL;
    if (job_list_is_empty(j)) return NULL;  // Not in list

    j->next->prev = j->prev;
    j->prev->next = j->next;

    job_list_reset(j);  // Detach
    return j;
}
```

### Buried Job Operations:
```c
// Check if any buried jobs
static int buried_job_p(Tube *t) {
    return !job_list_is_empty(&t->buried);
}

// Kick buried job (move to ready)
static int kick_buried_job(Server *s, Job *j) {
    remove_buried_job(j);  // Remove from list
    j->r.kick_ct++;
    return enqueue_job(s, j, 0, 1);
}

// Remove helper
static Job *remove_buried_job(Job *j) {
    if (!j || j->r.state != Buried) return NULL;
    j = job_list_remove(j);
    if (j) {
        global_stat.buried_ct--;
        j->tube->stat.buried_ct--;
    }
    return j;
}

// Kick N buried jobs
static uint kick_buried_jobs(Server *s, Tube *t, uint n) {
    uint i;
    for (i = 0; (i < n) && buried_job_p(t); ++i) {
        kick_buried_job(s, t->buried.next);  // Take from front
    }
    return i;
}
```

### ⚠️ Critical Details:
1. **Circular list**: Head points to itself when empty
2. **Header node**: `tube->buried` is NOT a real job, just a header
3. **FIFO order**: Kick takes from `buried.next` (front)
4. **LIFO bury**: New buried jobs inserted at `buried.prev` (tail)
5. **Double linkage**: Needed for O(1) removal given a job pointer

---

## 9. Delayed Jobs Processing

### Location: `prot.c`

Delayed jobs are stored in a **min-heap** per tube, ordered by `deadline_at`.

### Data Structure:
```c
struct Tube {
    Heap delay;  // Min-heap by deadline_at
};

// Heap ordering function
int job_delay_less(void *ja, void *jb) {
    Job *a = ja;
    Job *b = jb;
    if (a->r.deadline_at < b->r.deadline_at) return 1;
    if (a->r.deadline_at > b->r.deadline_at) return 0;
    return a->r.id < b->r.id;  // Tie-break by job ID
}
```

### Finding Soonest Delayed Job:
```c
// Across ALL tubes
static Job *soonest_delayed_job() {
    Job *j = NULL;
    size_t i;

    for (i = 0; i < tubes.len; i++) {
        Tube *t = tubes.items[i];
        if (t->delay.len == 0) continue;
        
        Job *nj = t->delay.data[0];  // Heap root
        if (!j || nj->r.deadline_at < j->r.deadline_at)
            j = nj;
    }
    return j;
}
```

### Processing in prottick:
```c
int64 prottick(Server *s) {
    int64 period = 0x34630B8A000LL;  // 1 hour default
    int64 now = nanoseconds();

    // Enqueue all jobs that are no longer delayed
    while ((j = soonest_delayed_job())) {
        d = j->r.deadline_at - now;
        if (d > 0) {
            period = min(period, d);  // Sleep until this job
            break;
        }
        // Job is ready now
        heapremove(&j->tube->delay, j->heap_index);
        int r = enqueue_job(s, j, 0, 0);  // Move to ready
        if (r < 1) bury_job(s, j, 0);  // OOM
    }
    
    return period;  // Time until next tick
}
```

### Kick Delayed Job:
```c
static int kick_delayed_job(Server *s, Job *j) {
    int z = walresvupdate(&s->wal);
    if (!z) return 0;
    j->walresv += z;

    heapremove(&j->tube->delay, j->heap_index);

    j->r.kick_ct++;
    int r = enqueue_job(s, j, 0, 1);
    if (r == 1) return 1;

    // Ready queue full - try to re-delay
    r = enqueue_job(s, j, j->r.delay, 0);
    if (r == 1) return 0;

    // Last resort - bury
    bury_job(s, j, 0);
    return 0;
}
```

### ⚠️ Critical Details:
1. **Per-tube heaps**: Each tube has its own delay heap
2. **Global scan**: `soonest_delayed_job()` scans all tubes O(num_tubes)
3. **Tie-break**: Jobs with same deadline ordered by ID
4. **heap_index**: Stored in job for O(log n) removal
5. **Fallback on kick failure**: Kick -> Delay -> Bury cascade

---

## 10. Potential Missing Features in Rust Port

Based on comparing the C source with typical Rust implementations, here are commonly missed features:

### 10.1 Job Hash Table
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Prime-based sizing | ✓ 48 primes (64-bit) | Often uses power-of-2 |
| Downscaling | ✓ at < 1/16 load factor | Often missing |
| Stack-initial array | ✓ `all_jobs_init[12289]` | Always heap-allocated |
| OOM tracking | ✓ `hash_table_was_oom` flag | Often missing |

**Recommendation**: Use `hashbrown` or similar with custom hasher, or implement prime-based table.

### 10.2 Fair Queue (ms_take)
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Round-robin | ✓ `last` index | Often uses VecDeque pop_front |
| O(1) removal | ✓ swap-with-last | Often shifts elements |
| Event callbacks | ✓ oninsert/onremove | Often missing |

**Recommendation**: The `backend/fair_queue.rs` should verify round-robin behavior.

### 10.3 Tube Pause
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Minimum 1ns | ✓ `delay = 1` if 0 | Often allows true 0 |
| Unpause at deadline | ✓ Checked in next_awaited_job | Often only in tick |

**Recommendation**: Check `src/architecture/tube.rs` for pause implementation.

### 10.4 WAL Reservation
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Pre-reservation | ✓ `walresvput`/`walresvupdate` | Often write-directly |
| Balance invariants | ✓ Congruence to delete size | Often missing |
| File migration | ✓ `moveone` for compaction | Often missing |
| Garbage collection | ✓ `walgc` on file unref | Often missing |

**Recommendation**: The `backup/binlog.rs` needs review for reservation system.

### 10.5 DEADLINE_SOON
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| 1s safety margin | ✓ `SAFETY_MARGIN` | Often missing or different |
| Pre-reserve check | ✓ Before wait_for_job | Often only during wait |
| Cached soonest_job | ✓ Avoids O(n) scans | Often recomputes |
| Early wake | ✓ `connsched` with margin | Often wakes at deadline |

**Recommendation**: Check `src/operation/dispatch.rs` for deadline handling.

### 10.6 Connection State Machine
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Epoll batching | ✓ `epollq` list | Often per-operation |
| State machine | ✓ Explicit states | Often implicit |
| Bitbucket mode | ✓ Discard oversized jobs | Often closes connection |
| Half-closed | ✓ `halfclosed` flag | Often missing |

### 10.7 Job State Management
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| reserver pointer | ✓ For ownership tracking | Often missing |
| List position cache | ✓ `heap_index`, `soonest_job` | Often missing |
| Copy state | ✓ For peek replies | Often clones |

### 10.8 Statistics
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Per-tube stats | ✓ `struct stats` in Tube | Often global only |
| Command counts | ✓ `op_ct[TOTAL_OPS]` | Often missing |
| Timeout count | ✓ `timeout_ct` | Often missing |
| WAL stats | ✓ `nmig`, `nrec` | Often missing |

### 10.9 Error Handling
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| OOM resilience | ✓ Bury on enqueue failure | Often panics/returns error |
| WAL disable | ✓ Sets `w->use = 0` on error | Often crashes |
| Partial write | ✓ `writev` with tracking | Often assumes full write |

### 10.10 Protocol Edge Cases
| Feature | C Implementation | Often Missed in Rust |
|---------|-----------------|---------------------|
| Command too long | ✓ STATE_WANT_ENDLINE | Often closes connection |
| NUL byte check | ✓ `strlen(c->cmd) != c->cmd_len - 2` | Often missing |
| Trailing garbage | ✓ Explicit checks | Often ignored |
| Unix sockets | ✓ `make_unix_socket` | Often TCP only |

---

## Summary

The beanstalkd C implementation contains several sophisticated mechanisms that are easy to miss in a Rust port:

1. **Prime-based hash table** with both upscaling and downscaling
2. **Round-robin fairness** in connection scheduling
3. **Pre-reservation WAL** with complex invariants
4. **Batched epoll** for performance
5. **DEADLINE_SOON** with 1-second safety margin
6. **Circular doubly-linked list** for buried jobs
7. **Multi-heap scan** for delayed jobs

A correct Rust port must carefully replicate these behaviors to maintain protocol compatibility and performance characteristics.
