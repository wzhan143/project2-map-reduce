#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

// Structure to pass data to mapper threads
typedef struct {
    mapper_t mapper;
    kvlist_t* input;
    kvlist_t* output;
} mapper_data_t;

// Structure to pass data to reducer threads  
typedef struct {
    reducer_t reducer;
    kvlist_t* input;
    kvlist_t* output;
} reducer_data_t;

// Mapper thread function
void* mapper_worker(void* arg) {
    mapper_data_t* data = (mapper_data_t*)arg;
    
    // Process each key-value pair in the input list
    kvlist_iterator_t* iter = kvlist_iterator_new(data->input);
    for (;;) {
        kvpair_t* pair = kvlist_iterator_next(iter);
        if (pair == NULL) {
            break;
        }
        // Call the mapper function for this pair
        data->mapper(pair, data->output);
    }
    kvlist_iterator_free(&iter);
    
    return NULL;
}

// Reducer thread function
void* reducer_worker(void* arg) {
    reducer_data_t* data = (reducer_data_t*)arg;
    
    // Sort the input list by key to group same keys together
    kvlist_sort(data->input);
    
    // Group by key and call reducer for each unique key
    kvlist_iterator_t* iter = kvlist_iterator_new(data->input);
    kvlist_t* current_group = kvlist_new();
    char* current_key = NULL;
    
    for (;;) {
        kvpair_t* pair = kvlist_iterator_next(iter);
        
        // If we reached end or found a new key, process current group
        if (pair == NULL || (current_key != NULL && strcmp(current_key, pair->key) != 0)) {
            if (current_key != NULL && current_group->head != NULL) {
                // Call reducer for the current key group
                data->reducer(current_key, current_group, data->output);
                
                // Clear the current group for next key
                kvlist_free(&current_group);
                current_group = kvlist_new();
            }
            
            if (pair == NULL) {
                break;
            }
        }
        
        // Update current key and add pair to current group
        if (current_key == NULL || strcmp(current_key, pair->key) != 0) {
            current_key = pair->key;
        }
        kvlist_append(current_group, kvpair_clone(pair));
    }
    
    kvlist_iterator_free(&iter);
    kvlist_free(&current_group);
    return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
    
    if (num_mapper == 0 || num_reducer == 0 || input == NULL || output == NULL) {
        return;
    }

    // Phase 1: Split Phase - Split input into num_mapper lists
    kvlist_t** mapper_inputs = malloc(num_mapper * sizeof(kvlist_t*));
    for (size_t i = 0; i < num_mapper; i++) {
        mapper_inputs[i] = kvlist_new();
    }
    
    // Distribute input pairs to mapper lists in round-robin fashion
    kvlist_iterator_t* input_iter = kvlist_iterator_new(input);
    size_t mapper_idx = 0;
    for (;;) {
        kvpair_t* pair = kvlist_iterator_next(input_iter);
        if (pair == NULL) {
            break;
        }
        kvlist_append(mapper_inputs[mapper_idx], kvpair_clone(pair));
        mapper_idx = (mapper_idx + 1) % num_mapper;
    }
    kvlist_iterator_free(&input_iter);

    // Phase 2: Map Phase - Spawn mapper threads
    pthread_t* mapper_threads = malloc(num_mapper * sizeof(pthread_t));
    mapper_data_t* mapper_args = malloc(num_mapper * sizeof(mapper_data_t));
    kvlist_t** mapper_outputs = malloc(num_mapper * sizeof(kvlist_t*));
    
    for (size_t i = 0; i < num_mapper; i++) {
        mapper_outputs[i] = kvlist_new();
        mapper_args[i] = (mapper_data_t){
            .mapper = mapper,
            .input = mapper_inputs[i],
            .output = mapper_outputs[i]
        };
        pthread_create(&mapper_threads[i], NULL, mapper_worker, &mapper_args[i]);
    }
    
    // Wait for all mapper threads to complete
    for (size_t i = 0; i < num_mapper; i++) {
        pthread_join(mapper_threads[i], NULL);
    }

    // Phase 3: Shuffle Phase - Distribute mapper outputs to reducer lists
    kvlist_t** reducer_inputs = malloc(num_reducer * sizeof(kvlist_t*));
    for (size_t i = 0; i < num_reducer; i++) {
        reducer_inputs[i] = kvlist_new();
    }
    
    // Use hash function to determine which reducer gets which key
    for (size_t i = 0; i < num_mapper; i++) {
        kvlist_iterator_t* output_iter = kvlist_iterator_new(mapper_outputs[i]);
        for (;;) {
            kvpair_t* pair = kvlist_iterator_next(output_iter);
            if (pair == NULL) {
                break;
            }
            unsigned long hash_val = hash(pair->key);
            size_t reducer_idx = hash_val % num_reducer;
            kvlist_append(reducer_inputs[reducer_idx], kvpair_clone(pair));
        }
        kvlist_iterator_free(&output_iter);
    }

    // Phase 4: Reduce Phase - Spawn reducer threads
    pthread_t* reducer_threads = malloc(num_reducer * sizeof(pthread_t));
    reducer_data_t* reducer_args = malloc(num_reducer * sizeof(reducer_data_t));
    kvlist_t** reducer_outputs = malloc(num_reducer * sizeof(kvlist_t*));
    
    for (size_t i = 0; i < num_reducer; i++) {
        reducer_outputs[i] = kvlist_new();
        reducer_args[i] = (reducer_data_t){
            .reducer = reducer,
            .input = reducer_inputs[i],
            .output = reducer_outputs[i]
        };
        pthread_create(&reducer_threads[i], NULL, reducer_worker, &reducer_args[i]);
    }
    
    // Wait for all reducer threads to complete
    for (size_t i = 0; i < num_reducer; i++) {
        pthread_join(reducer_threads[i], NULL);
    }

    // Phase 5: Combine all reducer outputs into final output
    for (size_t i = 0; i < num_reducer; i++) {
        kvlist_extend(output, reducer_outputs[i]);
    }

    // Cleanup
    for (size_t i = 0; i < num_mapper; i++) {
        kvlist_free(&mapper_inputs[i]);
        kvlist_free(&mapper_outputs[i]);
    }
    for (size_t i = 0; i < num_reducer; i++) {
        kvlist_free(&reducer_inputs[i]);
        kvlist_free(&reducer_outputs[i]);
    }
    
    free(mapper_inputs);
    free(mapper_threads);
    free(mapper_args);
    free(mapper_outputs);
    free(reducer_inputs);
    free(reducer_threads);
    free(reducer_args);
    free(reducer_outputs);
}
