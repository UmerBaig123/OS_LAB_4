// ...existing code...
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <sys/types.h>

// Message structure for the queue
typedef struct {
    int thread_id;
    int iteration;
    char content[601];
    struct timespec timestamp;
} message_t;

// Unbounded queue node structure with atomic next pointer
typedef struct queue_node {
    message_t* message;
    _Atomic(struct queue_node*) next;
} queue_node_t;

// Lock-free queue structure using atomic operations
typedef struct {
    _Atomic(queue_node_t*) head;
    _Atomic(queue_node_t*) tail;
    _Atomic(long) size;
} unbounded_queue_t;

unbounded_queue_t message_queue = {NULL, NULL, 0};

// Helper function to get current timestamp as formatted string
void get_current_time_str(char* buffer, size_t buffer_size) {
    struct timespec ts;
    struct tm* tm_info;
    
    clock_gettime(CLOCK_REALTIME, &ts);
    tm_info = localtime(&ts.tv_sec);
    
    snprintf(buffer, buffer_size, "[%04d-%02d-%02d %02d:%02d:%02d.%03ld]",
             tm_info->tm_year + 1900, tm_info->tm_mon + 1, tm_info->tm_mday,
             tm_info->tm_hour, tm_info->tm_min, tm_info->tm_sec,
             ts.tv_nsec / 1000000);
}

void push_to_queue(int thread_id, int iteration, const char* content) {
    // Create new message
    message_t* msg = malloc(sizeof(message_t));
    msg->thread_id = thread_id;
    msg->iteration = iteration;
    strncpy(msg->content, content, 600);
    msg->content[600] = '\0';
    clock_gettime(CLOCK_REALTIME, &msg->timestamp);
    
    // Create new queue node
    queue_node_t* new_node = malloc(sizeof(queue_node_t));
    new_node->message = msg;
    atomic_store(&new_node->next, NULL);
    
    // Lock-free enqueue: append to tail using compare-and-swap
    queue_node_t* old_tail;
    while (1) {
        old_tail = atomic_load(&message_queue.tail);
        
        // If tail is NULL, try to set it to new_node (empty queue case)
        if (old_tail == NULL) {
            if (atomic_compare_exchange_strong(&message_queue.head, &(queue_node_t*){NULL}, new_node)) {
                atomic_store(&message_queue.tail, new_node);
                atomic_fetch_add(&message_queue.size, 1);
                return;
            }
        } else {
            // Try to link new_node to the end
            queue_node_t* null_ptr = NULL;
            if (atomic_compare_exchange_strong(&old_tail->next, &null_ptr, new_node)) {
                // Successfully linked, now update tail
                atomic_compare_exchange_strong(&message_queue.tail, &old_tail, new_node);
                atomic_fetch_add(&message_queue.size, 1);
                return;
            }
        }
        // Retry if CAS failed
    }
}

message_t* pop_from_queue() {
    message_t* msg = NULL;
    queue_node_t* old_head;
    
    // Lock-free dequeue using compare-and-swap
    while (1) {
        old_head = atomic_load(&message_queue.head);
        
        // Wait if queue is empty (spin-wait with backoff)
        if (old_head == NULL) {
            // Small backoff to reduce CPU spinning
            usleep(1);
            continue;
        }
        
        // Try to move head to next node
        queue_node_t* next = atomic_load(&old_head->next);
        
        if (atomic_compare_exchange_strong(&message_queue.head, &old_head, next)) {
            // Successfully dequeued
            msg = old_head->message;
            atomic_fetch_sub(&message_queue.size, 1);
            
            // If queue becomes empty, update tail
            if (next == NULL) {
                queue_node_t* expected_tail = old_head;
                atomic_compare_exchange_strong(&message_queue.tail, &expected_tail, NULL);
            }
            
            free(old_head);
            return msg;
        }
        // Retry if CAS failed
    }
}

void* write_files_with_rate(void* arg) {
    int thread_index = *(int*)arg;
    free(arg);
    char content[601];
    struct timespec start_time;
    int iteration_index = 0;
    pthread_t thread_id = pthread_self();
    
    // Reduce content size to minimize memory usage
    memset(content, 'A', 200);  // Reduced from 600 to 200
    content[200] = '\0';
    
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    
    char time_str[64];
    get_current_time_str(time_str, sizeof(time_str));
    printf("%s Writer thread %lu (ID: %d) started\n", time_str, (unsigned long)thread_id, thread_index);
    
    while (1) {
        // Smaller message content
        snprintf(content, sizeof(content), 
                "Thread %d (pthread_id: %lu), Iteration %d", 
                thread_index, (unsigned long)thread_id, iteration_index);
        
        push_to_queue(thread_index, iteration_index, content);
        
        if (iteration_index % 1000 == 0) {  // Reduced logging frequency
            char time_str[64];
            get_current_time_str(time_str, sizeof(time_str));
            printf("%s Writer thread %d: Generated %d messages\n", time_str, thread_index, iteration_index + 1);
        }
        
        iteration_index++;
        usleep(10000);  // Increased from 1ms to 10ms to reduce message rate
    }
    
    return NULL;
}

void* file_writer_thread(void* arg) {
    message_t* msg;
    FILE* log_file;
    char time_str[64];
    pthread_t thread_id = pthread_self();
    struct timespec start_time, current_time;
    double elapsed_seconds;
    int messages_written = 0;
    
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    char startup_time_str[64];
    get_current_time_str(startup_time_str, sizeof(startup_time_str));
    printf("%s File writer thread %lu started\n", startup_time_str, (unsigned long)thread_id);
    
    // Keep file open for better performance
    log_file = fopen("log_thread.txt", "w");  // Start with empty file
    if (log_file == NULL) {
        printf("Error: Cannot open log_thread.txt\n");
        return NULL;
    }
    
    while (1) {
        msg = pop_from_queue();  // This will block until message is available
        
        clock_gettime(CLOCK_MONOTONIC, &current_time);
        elapsed_seconds = (current_time.tv_sec - start_time.tv_sec) + 
                         (current_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;
        
        // Write without additional mutex (only this thread writes)
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&msg->timestamp.tv_sec));
        
        fprintf(log_file, "[%s] Thread %d, Iteration %d (Elapsed: %.3f seconds):\n%s\n\n", 
               time_str, msg->thread_id, msg->iteration, elapsed_seconds, msg->content);
        
        // Flush every 100 messages instead of every message
        messages_written++;
        if (messages_written % 100 == 0) {
            fflush(log_file);
            
            char current_time_str[64];
            get_current_time_str(current_time_str, sizeof(current_time_str));
            printf("%s File writer: Wrote %d messages - Queue size: %ld\n", 
                   current_time_str, messages_written, atomic_load(&message_queue.size));
        }
        
        free(msg);
    }
    
    fclose(log_file);
    return NULL;
}



int main() {
    pthread_t writer_threads[2000];
    pthread_t file_writer_thread_id;
    char time_str[64];
    
    get_current_time_str(time_str, sizeof(time_str));
    printf("%s Starting FIFA Thread Writer with 2000 writer threads + 1 file writer thread\n", time_str);
    
    // Create the dedicated file writer thread
    if (pthread_create(&file_writer_thread_id, NULL, file_writer_thread, NULL) != 0) {
        get_current_time_str(time_str, sizeof(time_str));
        printf("%s Error creating file writer thread\n", time_str);
        return -1;
    }
     
    // Create 2000 writer threads
    for (int i = 0; i < 2000; i++) {
        int* thread_index = malloc(sizeof(int));
        *thread_index = i;
        
        if (pthread_create(&writer_threads[i], NULL, write_files_with_rate, thread_index) != 0) {
            get_current_time_str(time_str, sizeof(time_str));
            printf("%s Error creating writer thread %d\n", time_str, i);
            return -1;
        }
    }
    
    get_current_time_str(time_str, sizeof(time_str));
    printf("%s All threads created successfully!\n", time_str);
    printf("%s - 2000 writer threads generating content\n", time_str);
    printf("%s - 1 file writer thread writing to log_thread.txt\n", time_str);
    printf("%s Press Ctrl+C to stop...\n", time_str);
     
    // Join the file writer thread (this will run forever)
    pthread_join(file_writer_thread_id, NULL);
     
    // Join all writer threads (they also run forever)
    for (int i = 0; i < 2000; i++) {
        pthread_join(writer_threads[i], NULL);
    }
    
    return 0;
}
