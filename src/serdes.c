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

#include <stdarg.h>
#include <time.h>




int schema_traverse(const avro_schema_t schema, json_t *json,
                    avro_value_t *current_val, int quiet, int strjson, size_t max_str_sz)
{
        // fprintf(stderr, "Entering schema traverse\n");
        assert(json != NULL);
        assert(current_val != NULL);

        if (!json)
        {
                fprintf(stderr, "ERROR: Avro schema does not match JSON\n");
                return 1;
        }
        // fprintf(stderr, "schema type is %d\n", schema->type);
        switch (schema->type)
        {
        case AVRO_RECORD:
        {
                if (!json_is_object(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON object for Avro record, got something else\n");
                        return 1;
                }

                int len = avro_schema_record_size(schema), i;
                for (i = 0; i < len; i++)
                {

                        const char *name = avro_schema_record_field_name(schema, i);
                        avro_schema_t field_schema = avro_schema_record_field_get_by_index(schema, i);

                        json_t *json_val = json_object_get(json, name);

                        avro_value_t field;
                        avro_value_get_by_index(current_val, i, &field, NULL);

                        if (schema_traverse(field_schema, json_val, &field, quiet, strjson, max_str_sz))
                                return 1;
                }
        }
        break;

        case AVRO_LINK:
                /* TODO */
                fprintf(stderr, "ERROR: AVRO_LINK is not implemented\n");
                return 1;
                break;

        case AVRO_STRING:
                fprintf(stderr, "ERROR: IT'S AN AVRO STRING\n");
                if (!json_is_string(json))
                {
                        fprintf(stderr, "ERROR: NOT IS JSON STRING\n");
                        if (json && strjson)
                        {
                                fprintf(stderr, "ERROR: JSON AND STRJSON\n");
                                /* -j specified, just dump the remaining json as string */
                                char *js = json_dumps(json, JSON_COMPACT | JSON_SORT_KEYS | JSON_ENCODE_ANY);
                                fprintf(stderr, "AFTER JSON_DUMPS\n");
                                if (max_str_sz && (strlen(js) > max_str_sz))
                                        js[max_str_sz] = 0; /* truncate the string - this will result in invalid JSON! */
                                avro_value_set_string(current_val, js);
                                free(js);
                                break;
                        }
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON string for Avro string, got something else\n");
                        return 1;
                }
                else
                {
                        fprintf(stderr, "ELSE\n");
                        const char *js = json_string_value(json);
                        fprintf(stderr, "JS: x%sx\n", js);
                        if (max_str_sz && (strlen(js) > max_str_sz))
                        {
                                fprintf(stderr, "MAX STR SIZE\n");
                                /* truncate the string */
                                char *jst = malloc(strlen(js));
                                strcpy(jst, js);
                                jst[max_str_sz] = 0;
                                avro_value_set_string(current_val, jst);
                                free(jst);
                        }
                        else
                                fprintf(stderr, "avro_value_set_string\n");
                                avro_value_set_string(current_val, js);
                                fprintf(stderr, "AFTER avro_value_set_string\n");
                }
                break;

        case AVRO_BYTES:
                if (!json_is_string(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON string for Avro string, got something else\n");
                        return 1;
                }
                /* NB: Jansson uses null-terminated strings, so embedded nulls are NOT
                   supported, not even escaped ones */
                const char *s = json_string_value(json);
                avro_value_set_bytes(current_val, (void *)s, strlen(s));
                break;

        case AVRO_INT32:
                if (!json_is_integer(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON integer for Avro int, got something else\n");
                        return 1;
                }
                avro_value_set_int(current_val, json_integer_value(json));
                break;

        case AVRO_INT64:
                if (!json_is_integer(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON integer for Avro long, got something else\n");
                        return 1;
                }
                avro_value_set_long(current_val, json_integer_value(json));
                break;

        case AVRO_FLOAT:
                if (!json_is_number(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON number for Avro float, got something else\n");
                        return 1;
                }
                avro_value_set_float(current_val, json_number_value(json));
                break;

        case AVRO_DOUBLE:
                if (!json_is_number(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON number for Avro double, got something else\n");
                        return 1;
                }
                avro_value_set_double(current_val, json_number_value(json));
                break;

        case AVRO_BOOLEAN:
                if (!json_is_boolean(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON boolean for Avro boolean, got something else\n");
                        return 1;
                }
                avro_value_set_boolean(current_val, json_is_true(json));
                break;

        case AVRO_NULL:
                if (!json_is_null(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON null for Avro null, got something else\n");
                        return 1;
                }
                avro_value_set_null(current_val);
                break;

        case AVRO_ENUM:
                {
                json_int_t symbol_value;

                symbol_value = (json_int_t) avro_schema_enum_get_by_name(schema, json_string_value(json));
                avro_value_set_enum(current_val, symbol_value);
                }
                break;

        case AVRO_ARRAY:
                if (!json_is_array(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON array for Avro array, got something else\n");
                        return 1;
                }
                else
                {
                        int i, len = json_array_size(json);
                        avro_schema_t items = avro_schema_array_items(schema);
                        avro_value_t val;
                        for (i = 0; i < len; i++)
                        {
                                avro_value_append(current_val, &val, NULL);
                                if (schema_traverse(items, json_array_get(json, i), &val, quiet, strjson, max_str_sz))
                                        return 1;
                        }
                }
                break;

        case AVRO_MAP:
                if (!json_is_object(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON object for Avro map, got something else\n");
                        return 1;
                }
                else
                {
                        avro_schema_t values = avro_schema_map_values(schema);
                        void *iter = json_object_iter(json);
                        avro_value_t val;
                        while (iter)
                        {
                                avro_value_add(current_val, json_object_iter_key(iter), &val, 0, 0);
                                if (schema_traverse(values, json_object_iter_value(iter), &val, quiet, strjson, max_str_sz))
                                        return 1;
                                iter = json_object_iter_next(json, iter);
                        }
                }
                break;

        case AVRO_UNION:
        {
                unsigned int i;
                avro_value_t branch;
                for (i = 0; i < avro_schema_union_size(schema); i++)
                {
                        avro_value_set_branch(current_val, i, &branch);
                        avro_schema_t type = avro_schema_union_branch(schema, i);
                        if (!schema_traverse(type, json, &branch, 1, strjson, max_str_sz))
                                break;
                }
                if (i == avro_schema_union_size(schema))
                {
                        fprintf(stderr, "ERROR: No type in the Avro union matched the JSON type we got\n");
                        return 1;
                }
                break;
        }
        case AVRO_FIXED:
                if (!json_is_string(json))
                {
                        if (!quiet)
                                fprintf(stderr, "ERROR: Expecting JSON string for Avro fixed, got something else\n");
                        return 1;
                }
                /* NB: Jansson uses null-terminated strings, so embedded nulls are NOT
                   supported, not even escaped ones */
                const char *f = json_string_value(json);
                if (avro_value_set_fixed(current_val, (void *)f, strlen(f)))
                {
                        fprintf(stderr, "ERROR: Setting Avro fixed value FAILED\n");
                        return 1;
                }
                break;

        default:
                fprintf(stderr, "ERROR: Unknown type: %d\n", schema->type);
                return 1;
        }
        return 0;
}

void serdes_json_to_avro(char *buffer, int buffer_len, avro_schema_t schema, avro_value_t *val)
{
        clock_t begin = clock();

        // fprintf(stderr, "Entering serdes_json_to_avro\n");
        json_error_t err;
        json_t *json;
        int n = 0;
        int max_str_sz = 1024;
        int strjson = 1;

        assert(buffer != NULL);
        assert(val != NULL);
        // fprintf(stderr, "buffer: %s\n", buffer);
        json = json_loadb(buffer, buffer_len, 0, &err);
        if (!json)
        {
                fprintf(stderr, "JSON error on line %d, column %d, pos %d: %s, skipping to EOL\n", n, err.column, err.position, err.text);
        }

        if (schema_traverse(schema, json, val, 0, strjson, max_str_sz))
        {
                fprintf(stderr, "Error processing record %s, skipping...\n", buffer);
        }
        // fprintf(stderr, "Leaving serdes_json_to_avro\n");
        json_decref(json);
        // fprintf(stderr, "After json_decref\n");
        clock_t end = clock();
        double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;

        fprintf(stderr, "Time spent in json to avro: %f\n", time_spent);
}



const char *serdes_err2str (serdes_err_t err) {
        switch (err)
        {
        case SERDES_ERR_OK:
                return "Success";
        case SERDES_ERR_CONF_UNKNOWN:
                return "Unknown configuration property";
        case SERDES_ERR_CONF_INVALID:
                return "Invalid configuration property value";
        case SERDES_ERR_FRAMING_INVALID:
                return "Invalid payload framing";
        case SERDES_ERR_PAYLOAD_INVALID:
                return "Payload is invalid";
        case SERDES_ERR_SCHEMA_LOAD:
                return "Schema load failed";
        case SERDES_ERR_SCHEMA_MISMATCH:
                return "Object does not match schema";
        case SERDES_ERR_SCHEMA_REQUIRED:
                return "Schema required to perform operation";
        case SERDES_ERR_SERIALIZER:
                return "Serializer failed";
        case SERDES_ERR_BUFFER_SIZE:
                return "Inadequate buffer size";
        default:
                return "(unknown error)";
        }
}


static void serdes_conf_destroy0 (serdes_conf_t *sconf) {
        url_list_clear(&sconf->schema_registry_urls);
}

void serdes_conf_destroy (serdes_conf_t *sconf) {
        serdes_conf_destroy0(sconf);
        free(sconf);
}


/**
 * Low-level serdes_conf_t copy
 */
static void serdes_conf_copy0 (serdes_conf_t *dst, const serdes_conf_t *src) {

        url_list_clear(&dst->schema_registry_urls);
        if (src->schema_registry_urls.str)
                url_list_parse(&dst->schema_registry_urls,
                               src->schema_registry_urls.str);
        dst->serializer_framing   = src->serializer_framing;
        dst->deserializer_framing = src->deserializer_framing;
        dst->debug   = src->debug;
        dst->schema_load_cb = src->schema_load_cb;
        dst->schema_unload_cb = src->schema_unload_cb;
        dst->log_cb  = src->log_cb;
        dst->opaque = src->opaque;
}

serdes_conf_t *serdes_conf_copy (const serdes_conf_t *src) {
        serdes_conf_t *dst;
        dst = serdes_conf_new(NULL, 0, NULL);
        serdes_conf_copy0(dst, src);
        return dst;
}


serdes_err_t serdes_conf_set (serdes_conf_t *sconf,
                              const char *name, const char *val,
                              char *errstr, int errstr_size) {

        if (!strcmp(name, "schema.registry.url")) {
                url_list_clear(&sconf->schema_registry_urls);
                if (url_list_parse(&sconf->schema_registry_urls, val) == 0) {
                        snprintf(errstr, errstr_size,
                                 "Invalid value for %s", name);
                        return SERDES_ERR_CONF_INVALID;
                }

        } else if (!strcmp(name, "serializer.framing") ||
                   !strcmp(name, "deserializer.framing")) {
                int framing;
                if (!strcmp(val, "none"))
                        framing = SERDES_FRAMING_NONE;
                else if (!strcmp(val, "cp1"))
                        framing = SERDES_FRAMING_CP1;
                else {
                        snprintf(errstr, errstr_size,
                                 "Invalid value for %s, allowed values: "
                                 "cp1, none", name);
                        return SERDES_ERR_CONF_INVALID;
                }

                if (!strcmp(name, "serializer.framing"))
                        sconf->serializer_framing = framing;
                else
                        sconf->deserializer_framing = framing;

        } else if (!strcmp(name, "debug")) {
                if (!strcmp(val, "all"))
                        sconf->debug = 1;
                else if (!strcmp(val, "") || !strcmp(val, "none"))
                        sconf->debug = 0;
                else {
                        snprintf(errstr, errstr_size,
                                 "Invalid value for %s, allowed values: "
                                 "all, none", name);
                        return SERDES_ERR_CONF_INVALID;
                }
        } else {
                snprintf(errstr, errstr_size,
                         "Unknown configuration property %s", name);
                return SERDES_ERR_CONF_UNKNOWN;
        }

        return SERDES_ERR_OK;
}


void serdes_conf_set_schema_load_cb (serdes_conf_t *sconf,
                                     void *(*load_cb) (serdes_schema_t *schema,
                                                       const char *definition,
                                                       size_t definition_len,
                                                       char *errstr,
                                                       size_t errstr_size,
                                                       void *opaque),
                                     void (*unload_cb) (serdes_schema_t *schema,
                                                        void *schema_obj,
                                                        void *opaque)) {
        sconf->schema_load_cb = load_cb;
        sconf->schema_unload_cb = unload_cb;
}


void serdes_conf_set_log_cb (serdes_conf_t *sconf,
                             void (*log_cb) (serdes_t *sd,
                                             int level, const char *fac,
                                             const char *buf, void *opaque)) {
        sconf->log_cb     = log_cb;
}

void serdes_conf_set_opaque (serdes_conf_t *sconf, void *opaque) {
        sconf->opaque = opaque;
}


/**
 * Initialize config object to default values
 */
static void serdes_conf_init (serdes_conf_t *sconf) {
        memset(sconf, 0, sizeof(*sconf));
        sconf->serializer_framing   = SERDES_FRAMING_CP1;
        sconf->deserializer_framing = SERDES_FRAMING_CP1;
}

serdes_conf_t *serdes_conf_new (char *errstr, int errstr_size, ...) {
        serdes_conf_t *sconf;
        va_list ap;
        const char *name, *val;

        sconf = malloc(sizeof(*sconf));
        serdes_conf_init(sconf);

        /* Chew through name,value pairs. */
        va_start(ap, errstr_size);
        while ((name = va_arg(ap, const char *))) {
                if (!(val = va_arg(ap, const char *))) {
                        snprintf(errstr, errstr_size,
                                 "Missing value for \"%s\"", name);
                        serdes_conf_destroy(sconf);
                        return NULL;
                }
                if (serdes_conf_set(sconf, name, val, errstr, errstr_size) !=
                    SERDES_ERR_OK) {
                        serdes_conf_destroy(sconf);
                        return NULL;
                }
        }
        va_end(ap);

        return sconf;
}




void serdes_log (serdes_t *sd, int level, const char *fac,
                 const char *fmt, ...) {
        va_list ap;
        char buf[512];

        va_start(ap, fmt);
        vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);

        if (sd->sd_conf.log_cb)
                sd->sd_conf.log_cb(sd, level, fac, buf, sd->sd_conf.opaque);
        else
                fprintf(stderr, "%% SERDES-%d-%s: %s\n", level, fac, buf);
}


void serdes_destroy (serdes_t *sd) {
        serdes_schema_t *ss;
        fprintf(stderr, "serdes_destroy\n");
        while ((ss = LIST_FIRST(&sd->sd_schemas)))
                serdes_schema_destroy(ss);

        serdes_conf_destroy0(&sd->sd_conf);

        mtx_destroy(&sd->sd_lock);
        free(sd);
}

serdes_t *serdes_new (serdes_conf_t *conf, char *errstr, size_t errstr_size) {
        serdes_t *sd;
        fprintf(stderr, "serdes_new\n");
        sd = calloc(1, sizeof(*sd));
        LIST_INIT(&sd->sd_schemas);
        mtx_init(&sd->sd_lock, mtx_plain);

        if (conf) {
                serdes_conf_copy0(&sd->sd_conf, conf);
                serdes_conf_destroy(conf);
        } else
                serdes_conf_init(&sd->sd_conf);

        if (!sd->sd_conf.schema_load_cb) {
#ifndef ENABLE_AVRO_C
                snprintf(errstr, errstr_size,
                         "No schema loader configured"
                         "(serdes_conf_set_schema_load_cb)");
                serdes_destroy(sd);
                return NULL;
#else
                /* Default schema loader to Avro-C */
                sd->sd_conf.schema_load_cb   = serdes_avro_schema_load_cb;
                sd->sd_conf.schema_unload_cb = serdes_avro_schema_unload_cb;
#endif
        }

        return sd;
}
