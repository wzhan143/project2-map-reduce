#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

// If no time, don't need to worry about edge cases

// Structure to pass data to mapper threads
typedef struct {
  mapper_t mapper;   // User-supplied map function
  kvlist_t* input;   // Data assigned to this mapper
  kvlist_t* output;  // Store mapper's output pairs
} mapper_arg_t;

// Structure to pass data to reducer threads
typedef struct {
  reducer_t reducer;  // User-supplied reduce function
  kvlist_t* input;    // Data assigned to this reducer
  kvlist_t* output;   // Store reducer's output pairs
} reducer_arg_t;

// Shuffle struct maybe not needed? Make just in case why not XD
typedef struct {
  kvlist_t** reducer_lists;
  size_t num_reducer;
  pthread_mutex_t* mutexes;
} shuffle_data_t;

// Mapper thread function
void* mapper_thread(void* arg) {
  mapper_arg_t* m_arg = (mapper_arg_t*)arg;

  // Process each key-value pair in input list
  kvlist_iterator_t* itor = kvlist_iterator_new(m_arg->input);
  for (;;) {
    kvpair_t* pair = kvlist_iterator_next(itor);
    if (pair == NULL) {
      break;
    }
    // Use mapper function here
    m_arg->mapper(pair, m_arg->output);
  }
  kvlist_iterator_free(&itor);
  return NULL;
}

// Reducer thread function
void* reducer_thread(void* arg) {
  reducer_arg_t* r_arg = (reducer_arg_t*)arg;

  // Sort the input list by key to group same keys together
  kvlist_sort(r_arg->input);

  // Group by key and call reducer for each unique key
  kvlist_iterator_t* itor = kvlist_iterator_new(r_arg->input);
  kvlist_t* current_group = kvlist_new();
  char* current_key = NULL;

  for (;;) {
    kvpair_t* pair = kvlist_iterator_next(itor);

    // If we reached end or found a new key, process current group
    if (pair == NULL || (current_key != NULL && strcmp(current_key, pair->key) != 0)) {
      if (current_key != NULL && current_group->head != NULL) {
        // Call reducer for current key group
        r_arg->reducer(current_key, current_group, r_arg->output);

        // Clear current group for next key
        kvlist_free(&current_group);
        current_group = kvlist_new();
      }

      // Need to break?
      if (pair == NULL) {
        break;
      }
    }

    // Now need to update current key and add pair to current group
    if (current_key == NULL || strcmp(current_key, pair->key) != 0) {
      current_key = pair->key;
    }
    kvlist_append(current_group, kvpair_clone(pair));
  }

  kvlist_iterator_free(&itor);
  kvlist_free(&current_group);
  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  // Check requirements
  if (num_mapper == 0 || num_reducer == 0 || input == NULL || output == NULL) {
    return;
  }

  // 1. Split Phase: Split input into num_mapper lists
  kvlist_t** mapper_inputs = malloc(num_mapper * sizeof(kvlist_t*));
  for (size_t i = 0; i < num_mapper; i++) {
    mapper_inputs[i] = kvlist_new();
  }

  // Distribute input pairs to mapper lists using round-robin
  kvlist_iterator_t* input_itor = kvlist_iterator_new(input);
  size_t mapper_index = 0;
  for (;;) {
    kvpair_t* pair = kvlist_iterator_next(input_itor);
    if (pair == NULL) {
      break;
    }
    kvlist_append(mapper_inputs[mapper_index], kvpair_clone(pair));
    mapper_index = (mapper_index + 1) % num_mapper;
  }
  kvlist_iterator_free(&input_itor);

  // 2. Map Phase: Mapper thread setup
  pthread_t* mapper_threads = malloc(num_mapper * sizeof(pthread_t));
  mapper_arg_t* mapper_args = malloc(num_mapper * sizeof(mapper_arg_t));
  kvlist_t** mapper_outputs = malloc(num_mapper * sizeof(kvlist_t*));

  for (size_t i = 0; i < num_mapper; ++i) {
    mapper_outputs[i] = kvlist_new();

    mapper_args[i].mapper = mapper;
    mapper_args[i].input = mapper_inputs[i];
    mapper_args[i].output = mapper_outputs[i];

    pthread_create(&mapper_threads[i], NULL, mapper_thread, &mapper_args[i]);
  }

  // Wait for all mapper threads to finish
  for (size_t i = 0; i < num_mapper; ++i) {
    pthread_join(mapper_threads[i], NULL);
  }

  // 3. Shuffle Phase: Distribute mapper outputs to reducer lists
  kvlist_t** reducer_inputs = malloc(num_reducer * sizeof(kvlist_t*));
  for (size_t i = 0; i < num_reducer; i++) {
    reducer_inputs[i] = kvlist_new();
  }

  // Using hash to route keys
  for (size_t i = 0; i < num_mapper; ++i) {
    kvlist_iterator_t* out_itor = kvlist_iterator_new(mapper_outputs[i]);
    kvpair_t* pair;
    while ((pair = kvlist_iterator_next(out_itor)) != NULL) {
      unsigned long hval = hash(pair->key);
      size_t idx = hval % num_reducer;
      kvlist_append(reducer_inputs[idx], kvpair_clone(pair));
    }
    kvlist_iterator_free(&out_itor);
  }

  // 4. Reduce Phase: Reducer thread setup
  pthread_t* reducer_threads = malloc(num_reducer * sizeof(pthread_t));
  reducer_arg_t* reducer_args = malloc(num_reducer * sizeof(reducer_arg_t));
  kvlist_t** reducer_outputs = malloc(num_reducer * sizeof(kvlist_t*));

  for (size_t i = 0; i < num_reducer; ++i) {
    reducer_outputs[i] = kvlist_new();

    reducer_args[i].reducer = reducer;
    reducer_args[i].input = reducer_inputs[i];
    reducer_args[i].output = reducer_outputs[i];

    pthread_create(&reducer_threads[i], NULL, reducer_thread, &reducer_args[i]);
  }

  // Is this needed?? YES - wait for all reducer threads to finish
  for (size_t i = 0; i < num_reducer; ++i) {
    pthread_join(reducer_threads[i], NULL);
  }

  // 5. Combine all reducer outputs into final output
  for (size_t i = 0; i < num_reducer; ++i) {
    kvlist_extend(output, reducer_outputs[i]);
  }

  // Free stuff bye bye
  for (size_t i = 0; i < num_mapper; ++i) {
    kvlist_free(&mapper_inputs[i]);
    kvlist_free(&mapper_outputs[i]);
  }
  for (size_t i = 0; i < num_reducer; ++i) {
    kvlist_free(&reducer_inputs[i]);
    kvlist_free(&reducer_outputs[i]);
  }

  // Free all allocated arrays
  free(mapper_inputs);
  free(mapper_threads);
  free(mapper_args);
  free(mapper_outputs);
  free(reducer_inputs);
  free(reducer_threads);
  free(reducer_args);
  free(reducer_outputs);
}
