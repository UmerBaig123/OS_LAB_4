#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
int write_to_file(const char* filename, const char* content) {
    FILE* file = fopen(filename, "w");
    if (file == NULL) {
        printf("Error: Could not open file %s\n", filename);
        return -1;
    }
    
    fprintf(file, "%s", content);
    fclose(file);
    
    return 0;
}

// Global queue structure (simple array-based queue)
#define MAX_QUEUE_SIZE 10000
char* filename_queue[MAX_QUEUE_SIZE];
int queue_front = 0;
int queue_rear = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

void push_to_queue(const char* filename) {
    pthread_mutex_lock(&queue_mutex);
    if ((queue_rear + 1) % MAX_QUEUE_SIZE != queue_front) {
        filename_queue[queue_rear] = malloc(strlen(filename) + 1);
        strcpy(filename_queue[queue_rear], filename);
        queue_rear = (queue_rear + 1) % MAX_QUEUE_SIZE;
    }
    pthread_mutex_unlock(&queue_mutex);
}

void write_files_with_rate(int thread_index) {
    char filename[256];
    char content[6001]; // 6000 bytes + null terminator
    struct timespec start_time, current_time;
    int iteration_index = 0;
    
    // Fill content with 600 bytes
    memset(content, 'A', 600);
    content[6000] = '\0';
    
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    
    while (1) {
        snprintf(filename, sizeof(filename), "log_%d_%d", thread_index, iteration_index);
        
        if (write_to_file(filename, content) == 0) {
            push_to_queue(filename);
        }
        
        iteration_index++;
        
        usleep(1000);
    }
}
char* pop_from_queue() {
    char* filename = NULL;
    pthread_mutex_lock(&queue_mutex);
    if (queue_front != queue_rear) {
        filename = filename_queue[queue_front];
        queue_front = (queue_front + 1) % MAX_QUEUE_SIZE;
    }
    pthread_mutex_unlock(&queue_mutex);
    return filename;
}

void read_files_from_queue() {
    char* filename;
    FILE* file;
    char buffer[6001];
    
    while (1) {
        filename = pop_from_queue();
        if (filename != NULL) {
            file = fopen(filename, "r");
            if (file != NULL) {
                if (fread(buffer, 1, 600, file) > 0) {
                    buffer[600] = '\0';
                    printf("Read file: %s\n", filename);
                }
                fclose(file);
            }
            free(filename);
        } else {
            usleep(1000); // Sleep briefly if queue is empty
        }
    }
}

int main() {
    pthread_t writer_threads[2000];
    pthread_t reader_thread;
    
    // Create reader thread
    if (pthread_create(&reader_thread, NULL, (void*)read_files_from_queue, NULL) != 0) {
        printf("Error creating reader thread\n");
        return -1;
    }
    
    // Create 2000 writer threads
    for (int i = 0; i < 2000; i++) {
        int* thread_index = malloc(sizeof(int));
        *thread_index = i;
        
        if (pthread_create(&writer_threads[i], NULL, (void*)write_files_with_rate, thread_index) != 0) {
            printf("Error creating writer thread %d\n", i);
            return -1;
        }
    }
    
    // Wait for reader thread (runs indefinitely)
    pthread_join(reader_thread, NULL);
    
    // Wait for writer threads (runs indefinitely)
    for (int i = 0; i < 2000; i++) {
        pthread_join(writer_threads[i], NULL);
    }
    
    return 0;
}
