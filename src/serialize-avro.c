/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "serdes_int.h"
#include "serdes-avro.h"
#include <time.h>





serdes_err_t serdes_schema_serialize_avro (serdes_schema_t *ss,
                                           avro_value_t *avro,
                                           void **payloadp, size_t *sizep,
                                           char *errstr, int errstr_size) {

        
        clock_t begin = clock();
        
        char *payload;
        size_t size;
        avro_writer_t writer;
        int aerr;
        ssize_t of;
        fprintf(stderr, "serdes_schema_serialize_avro\n");
        /* Serialized output size */
        aerr = avro_value_sizeof(avro, &size);
        if (aerr) {
                snprintf(errstr, errstr_size,
                         "avro_value_sizeof() failed: %s",
                         strerror(aerr));
                return SERDES_ERR_SERIALIZER;
        }
        fprintf(stderr, "before serdes serializer framing size\n");
        size += serdes_serializer_framing_size(ss->ss_sd);
        fprintf(stderr, "After serdes serializer framing size\n");
        if (!payloadp) {
                /* Application is querying for buffer size */
                fprintf(stderr, "querying for buffer size\n");
                *sizep = size;
                return SERDES_ERR_OK;

        } else if (*payloadp) {
                /* Application provided a buffer */
                fprintf(stderr, "application provided buffer\n");
                if (*sizep < size) {
                        /* Make sure application's buffer is large enough */
                        snprintf(errstr, errstr_size,
                                 "Provided buffer size %zd < required "
                                 "buffer size %zd",
                                 *sizep, size);
                        return SERDES_ERR_BUFFER_SIZE;
                }

                payload = *payloadp;
        } else {
                fprintf(stderr, "allocating buffer\n");
                /* Allocate buffer */
                payload = malloc(size);
        }

        /* Write framing, if any. */
        fprintf(stderr, "before serdes framing write\n");
        of = serdes_framing_write(ss, payload, size);
        if (of == -1) {
                snprintf(errstr, errstr_size, "Not enough space for framing");
                if (!*payloadp)
                        free(payload);
                return SERDES_ERR_BUFFER_SIZE;
        }


        /* Create Avro serializer */
        fprintf(stderr, "before avro writer memory\n");
        writer = avro_writer_memory(payload+of, size-of);

        // FIXME: Schema validation

        /* Serialize Avro object */
        fprintf(stderr, "before avro value write\n");
        if (avro_value_write(writer, avro)) {
                snprintf(errstr, errstr_size,
                         "Failed to write Avro value: %s", avro_strerror());
                avro_writer_free(writer);
                if (!*payloadp)
                        free(payload);
                return SERDES_ERR_SERIALIZER;
        }

        /* Return buffer and size to application */
        fprintf(stderr, "before avro writer tell\n");
        *payloadp = payload;
        *sizep = of + avro_writer_tell(writer);

        avro_writer_free(writer);
        fprintf(stderr, "after avro writer free\n");
        fprintf(stderr, "returning serdes err ok\n");

        clock_t end = clock();
        double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;

        fprintf(stderr, "Time spent in serialize: %f\n", time_spent);

        return SERDES_ERR_OK;
        
}

