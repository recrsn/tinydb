#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>
#include <inttypes.h>

//#define DEBUG
#define QUIET

#define INPUT_BUFFER_SIZE 2048
#define MAX_TABLE_NAME_SIZE 32
#define MAX_FIELD_NAME_SIZE 32
#define MAX_TABLE_FIELDS 32
#define MAX_FIELD_LENGTH 2048
#define SELECT_MAX 32

#define starts_with(x, y) (strncmp(x, y, strlen(x)) == 0)

typedef enum {
    field_type_char = 0,
    field_type_integer = 1,
    field_type_undefined = 999,
} field_type;

typedef struct {
    char name[MAX_FIELD_NAME_SIZE];
    size_t length;
    field_type type;
} field;

typedef struct {
    char name[MAX_TABLE_NAME_SIZE];
    int n_fields;
    field fields[MAX_TABLE_FIELDS];
    size_t n_rows;
} table_info;

typedef struct {
    table_info info;
    bool temporary;
    FILE *fp;
    uint8_t *data;
} table;

typedef struct {
    char table[MAX_TABLE_NAME_SIZE];
    char field[MAX_FIELD_NAME_SIZE];
    int col;
} query_field;

typedef enum {
    literal_type_constant,
    literal_type_field
} literal_type;

typedef struct {
    literal_type type;
    char value[MAX_FIELD_LENGTH];
    table *table;
    int col;
} literal;

typedef enum {
    conjunction_or,
    conjunction_and
} conjunction;

typedef enum {
    operator_eq,
    operator_undefined,
} condition_operator;

typedef struct {
    literal literal1;
    literal literal2;
    conjunction conjunction;
    condition_operator operator;
} query_condition;

typedef struct {
    int n_tables;
    int n_fields;
    int n_conditions;
    table *tables[SELECT_MAX];
    query_field fields[SELECT_MAX];
    query_condition conditions[SELECT_MAX];
} query;

typedef struct {
    table *table;
    bool *include_rows;
} result_set;

bool show_table_info(table_info t);

char *str_trim(char *str) {
    char *end;

    // Trim leading space
    while (isspace((unsigned char) *str)) {
        str++;
    }

    if (*str == 0) // All spaces?
        return str;

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char) *end)) {
        end--;
    }

    // Write new null terminator
    *(end + 1) = 0;

    return str;
}

field_type str_to_field_type(const char *type) {
    if (strcasecmp("char", type) == 0) {
        return field_type_char;
    } else if (strcasecmp("int", type) == 0) {
        return field_type_integer;
    }

    return field_type_undefined;
}

char *field_type_to_str(field_type type) {
    switch (type) {
        case field_type_integer:
            return "int";
        case field_type_char:
            return "char";
        case field_type_undefined:
        default:
            return "undefined";
    }
}

bool read_table_info(table_info *t, const char *name) {

    char fname[MAX_TABLE_NAME_SIZE + 6];
    sprintf(fname, "%s.table", name);

    FILE *fp = fopen(fname, "rb");

    if (NULL == fp) {
        return false;
    }

    fread(t, sizeof(table_info), 1, fp);

    if (ferror(fp)) {
        return false;
    }

    fclose(fp);
    return true;

}

bool write_table_info(const table_info *t) {

#ifdef DEBUG
    printf("Writing table: %s Fields: %d Rows: %lu\n", t->name, t->n_fields, t->n_rows);
#endif

    char fname[MAX_TABLE_NAME_SIZE + 6];
    sprintf(fname, "%s.table", t->name);

    FILE *fp = fopen(fname, "w");

    if (fp == NULL) {
        return false;
    }

    fwrite(t, sizeof(table_info), 1, fp);

    if (ferror(fp)) {
        return false;
    }

    fclose(fp);

    return true;
}

int table_find_field(table_info t, const char *field_name) {
    for (int i = 0; i < t.n_fields; i++) {
        if (strcmp(t.fields[i].name, field_name) == 0) {
            return i;
        }
    }

    return -1;
}

size_t field_size(field f) {
    switch (f.type) {
        case field_type_char:
            return f.length + 1;
        case field_type_integer:
            return sizeof(int64_t);
        case field_type_undefined:
        default:
            return 0;
    }
}

size_t row_size(const table_info t) {
    size_t size = 0;
    for (int i = 0; i < t.n_fields; i++) {
        size += field_size(t.fields[i]);
    }
    return size;
}

size_t row_size_2(int n_fields, const field *fields) {
    size_t size = 0;
    for (int i = 0; i < n_fields; i++) {
        size += field_size(fields[i]);
    }
    return size;
}

size_t mem_offset(int n_fields, const field *fields, size_t row, int col) {
    size_t size = row * row_size_2(n_fields, fields);
    for (int i = 0; i < col; i++) {
        size += field_size(fields[i]);
    }
    return size;
}

size_t seek_pos(const table_info t, size_t row, int col) {
    return mem_offset(t.n_fields, t.fields, row, col);
}

table *open_table(const char *name) {
    table *t = (table *) malloc(sizeof(table));

    //read_table_info(&t->info, name);
    char fname[FILENAME_MAX];
    sprintf(fname, "%s.table", name);

    FILE *fp = fopen(fname, "rb");

    if (NULL == fp) {
        return false;
    }

    fread(&t->info, sizeof(table_info), 1, fp);
    fclose(fp);

    //char fname[MAX_TABLE_NAME_SIZE + 4];
    sprintf(fname, "%s.bin", name);

    t->fp = fopen(fname, "a+");

    if (NULL == t->fp) {
        perror("Error opening table");
        //free(t);
        return NULL;
    }

    return t;
}

void close_table(table *t) {
    fclose(t->fp);
    free(t);
}

bool write_table_row(const table *t, size_t row, uint8_t *data) {

    fseek(t->fp, seek_pos(t->info, row, 0), SEEK_SET);
    fwrite(data, sizeof(uint8_t), row_size(t->info), t->fp);

    if (ferror(t->fp)) {
        return false;
    }
    //puts("Written");
    return true;
}

bool parse_create(const char *input) {
    //puts("CREATE");
    int n;
    table_info t_info;
    memset(&t_info, 0, sizeof(table_info));
    t_info.n_fields = 0;
    t_info.n_rows = 0;
    n = sscanf(input, "CREATE TABLE %s", t_info.name);

    if (n != 1) {
        return false;
    }

    char ib[INPUT_BUFFER_SIZE];
    int i = 0;

    do {
        fgets(ib, INPUT_BUFFER_SIZE, stdin);
        str_trim(ib);

#ifndef QUIET
        printf("===> %s\n", ib);
#endif

        if (starts_with("ADD", ib)) {
            char field_type[16];
            n = sscanf(ib, "ADD %s %s %lu", t_info.fields[i].name, field_type, &t_info.fields[i].length);
            if (n != 3) {
                return false;
            }
            t_info.fields[i].type = str_to_field_type(field_type);
            i++;
            t_info.n_fields++;
        }

    } while (strcmp(ib, "END") != 0);

    return write_table_info(&t_info);
}

bool parse_insert(const char *input) {
    char table_name[MAX_TABLE_NAME_SIZE];
    char insert_data[INPUT_BUFFER_SIZE];
    int n = sscanf(input, "INSERT INTO %s %[^\n]%*c", table_name, insert_data);
    if (n != 2) {
        return false;
    }

    table *t = open_table(table_name);
    if (t != NULL) {
        //show_table_info(&t);
        uint8_t *values = calloc(row_size(t->info), 1);

        int index = 0;
        for (int i = 0; i < t->info.n_fields; i++) {
            char *tok = strtok(i == 0 ? insert_data : NULL, ",");

            int64_t long_val;
            size_t f_size = field_size(t->info.fields[i]);
            switch (t->info.fields[i].type) {
                case field_type_char:
                    memcpy(values + index, tok, strlen(tok));
                    break;
                case field_type_integer:
                    long_val = strtoll(tok, NULL, 10);
                    memcpy(values + index, &long_val, f_size);
                    break;
                case field_type_undefined:
                    // do nothing
                    break;
            }
            index += f_size;
        }

        write_table_row(t, t->info.n_rows++, values);
        write_table_info(&(t->info));
        close_table(t);
        return true;
    }

    return false;
}

bool parse_delete(const char *input) {
    return true;
}

void decode_field(char *output, const uint8_t *raw, field_type f) {
    switch (f) {
        case field_type_char:
            strcpy(output, (char *) raw);
            break;
        case field_type_integer:
            sprintf(output, "%" PRIi64, *((int64_t *) raw));
        case field_type_undefined:
            sprintf(output, "undefined");
    }
}

bool read_field(uint8_t *raw, const table *t, size_t row, int col) {
    size_t size = field_size(t->info.fields[col]);
    fseek(t->fp, seek_pos(t->info, row, col), SEEK_SET);
    fread(raw, size, 1, t->fp);

    return !ferror(t->fp);
}

table *create_temp_table(int n_fields, field fields[], size_t n_rows) {
    table *temp = malloc(sizeof(table));
    temp->data = malloc(row_size_2(n_fields, fields) * n_rows * sizeof(uint8_t));
    temp->info.n_rows = 0;
    temp->info.n_fields = n_fields;
    temp->temporary = true;
    memcpy(temp->info.fields, fields, sizeof(field) * n_fields);
    return temp;
}

void set_temp_table_field(table *t, size_t row, int col, uint8_t *value) {
    size_t offset = mem_offset(t->info.n_fields, t->info.fields, row, col);
    memcpy(t->data + offset, value, field_size(t->info.fields[col]));
}

void decode_temp_table_field(char *dest, table *t, size_t row, int col) {
    size_t offset = mem_offset(t->info.n_fields, t->info.fields, row, col);
    uint8_t *value = alloca(field_size(t->info.fields[col]));
    memcpy(value, t->data + offset, field_size(t->info.fields[col]));
    decode_field(dest, value, t->info.fields[col].type);
}

bool index_query(query q) {
    // only binary search for 1 row
    table *t = q.tables[0];
    char buf[MAX_FIELD_LENGTH];

    if(q.n_conditions > 0) {
        query_condition c = q.conditions[0];

        size_t l = 0;
        size_t r = t->info.n_rows - 1;
        while (l <= r) {
            size_t m = l + (r - l) / 2;

            printf("TRACE: ");
            for (int j = 0; j < q.n_fields; j++) {
                decode_temp_table_field(buf, t, m, q.fields[j].col);
                printf("%s%s", j == 0 ? "" : ",", buf);
            }
            puts("");

            decode_temp_table_field(buf, t, m, 0);
            int diff = strcmp(c.literal2.value, buf);
            // Check if x is present at mid
            if (diff == 0) {
                for (int j = 0; j < q.n_fields; j++) {
                    decode_temp_table_field(buf, t, m, q.fields[j].col);
                    printf("%s%s", j == 0 ? "" : ",", buf);
                }
                puts("");
                return true;
            }

            if (diff > 0) {
                l = m + 1;
            } else {
                r = m - 1;
            }
        }
    } else {
        for (int i = 0; i < t->info.n_rows ; ++i) {
            for (int j = 0; j < q.n_fields; j++) {
                decode_temp_table_field(buf, t, i, q.fields[j].col);
                printf("%s%s", j == 0 ? "" : ",", buf);
            }
            puts("");
        }
    }

}

bool single_query(query q) {
    table *t = q.tables[0];

    if (t->temporary) {
        return index_query(q);
    }

    uint8_t data[MAX_FIELD_LENGTH];

    for (size_t i = 0; i < t->info.n_rows; i++) {
        // condition
        bool accept = true;
        for (int k = 0; k < q.n_conditions; k++) {
            char val1[MAX_FIELD_LENGTH];
            char val2[MAX_FIELD_LENGTH];

            if (q.conditions[k].literal1.type == literal_type_field) {
                read_field(data, t, i, q.conditions[k].literal1.col);
                field_type type = t->info.fields[q.conditions[k].literal1.col].type;
                decode_field(val1, data, type);
            } else {
                strcpy(val1, q.conditions[k].literal1.value);
                val1[strlen(val1) - 1] = 0;
            }

            if (q.conditions[k].literal2.type == literal_type_field) {
                read_field(data, t, i, q.conditions[k].literal2.col);
                field_type type = t->info.fields[q.conditions[k].literal2.col].type;
                decode_field(val2, data, type);
            } else {
                strcpy(val2, q.conditions[k].literal2.value);
            }

            // since we only support equals now
            bool result = strcmp(val1, val2) == 0;

            accept = q.conditions[k].conjunction == conjunction_and ? accept && result : accept || result;

        }

        if (!accept) {
            continue;
        }

        for (int j = 0; j < q.n_fields; j++) {
            //uint8_t data[MAX_FIELD_LENGTH];
            char decoded[MAX_FIELD_LENGTH];
            read_field(data, t, i, q.fields[j].col);
            field_type type = t->info.fields[q.fields[j].col].type;
            decode_field(decoded, data, type);
            printf("%s%s", j == 0 ? "" : ",", decoded);
        }
        puts("");
    }

    return true;
}

void parse_query_literal(literal *lit, const char *op, table *tables[], int n_tables) {
    lit->type = op[0] == '"' || isdigit(op[0]) ? literal_type_constant : literal_type_field;
    if (lit->type == literal_type_field) {
        for (int i = 0; i < n_tables; i++) {
            int col = table_find_field(tables[i]->info, op);
            if (col != -1) {
                lit->table = tables[i];
                lit->col = col;
            }
        }
    }

    if (lit->type == literal_type_constant && op[0] == '"') {
        strcpy(lit->value, op + 1);
        lit->value[strlen(lit->value) - 1] = 0;
    } else {
        strcpy(lit->value, op);
    }
}

void parse_query_operator(condition_operator *lit, const char *op) {
    if (strcmp(op, "=") == 0) {
        *lit = operator_eq;
    } else {
        *lit = operator_undefined;
    }
}

bool has_self_join(query q) {
//    for(int i = 0; i < q.n_conditions; i++) {
//        if(q.conditions[i].literal1.type == literal_type_field && q.conditions[i].literal2.type == literal_type_field) {
//
//        }
//    }
    return false;
}

void filter(bool *include, table *t, int col, const char *val) {
    char field_val[MAX_FIELD_LENGTH];
    uint8_t data[MAX_FIELD_LENGTH];
    field_type type = t->info.fields[col].type;
    for (int i = 0; i < t->info.n_rows; i++) {
        read_field(data, t, i, col);
        decode_field(field_val, data, type);
        *include &= strcmp(field_val, val) == 0;
        include++;
    }
}

result_set *create_result_set(table *t) {
    result_set *rs = malloc(sizeof(result_set));
    rs->table = t;
    rs->include_rows = malloc(t->info.n_rows * sizeof(bool));
    for (int i = 0; i < t->info.n_rows * sizeof(bool); ++i) {
        rs->include_rows[i] = true;
    }
    return rs;
}

result_set *get_result_set(result_set **rs, int rs_size, table *t) {
    for (int i = 0; i < rs_size; i++) {
        if (rs[i]->table == t) {
            return rs[i];
        }
    }
    fputs("Result set not found", stderr);
    return NULL;
}

size_t count_included_rows(size_t n, const bool *array) {
    size_t x = 0;
    for (size_t i = 0; i < n; ++i) {
        if (array[i]) {
            x++;
        }
    }

    return x;
}

table *table_to_temp_table(table *t, const bool *include_rows) {
    table *tmp = create_temp_table(t->info.n_fields, t->info.fields,
                                   count_included_rows(t->info.n_rows, include_rows));
    size_t index = 0;
    for (size_t i = 0; i < t->info.n_rows; i++) {
        if (include_rows[i]) {
            size_t offset = mem_offset(t->info.n_fields, t->info.fields, index++, 0);
            size_t seek = seek_pos(t->info, i, 0);
            size_t row_sz = row_size(t->info) * sizeof(uint8_t);
            fseek(t->fp, seek, SEEK_SET);
            fread(tmp->data + offset, row_sz, 1, t->fp);
        }
    }
    tmp->info.n_rows = index;
    return tmp;
}

table *temp_table_inner_join(table *table_a, int col_a,
                             table *table_b, int col_b) {
    char val1[MAX_FIELD_LENGTH];
    char val2[MAX_FIELD_LENGTH];

    int n_fields = table_a->info.n_fields + table_b->info.n_fields;
    field fields[n_fields];

    memcpy(fields, table_a->info.fields, table_a->info.n_fields * sizeof(field));
    memcpy(fields + table_a->info.n_fields, table_b->info.fields, table_b->info.n_fields * sizeof(field));

//#ifdef DEBUG
//    for (int i = 0; i < n_fields; i++) {
//        printf("%s\t%s(%lu)\n", fields[i].name, field_type_to_str(fields[i].type),
//               fields[i].length);
//    }
//    printf("-----PARSED FIELDS-----\n");
//#endif

    table *tmp = create_temp_table(n_fields, fields, table_a->info.n_rows * table_b->info.n_rows);
    tmp->info.n_rows = 0;

    for (size_t i = 0; i < table_a->info.n_rows; i++) {
        decode_temp_table_field(val1, table_a, i, col_a);
        size_t table_a_offset = row_size(table_a->info) * i;
        for (size_t j = 0; j < table_b->info.n_rows; j++) {
            decode_temp_table_field(val2, table_b, j, col_b);
            size_t table_b_offset = row_size(table_b->info) * j;
            if (strcmp(val1, val2) == 0) {
#ifdef DEBUG
                printf("MATCH %s %s\n", val1, val2);
#endif
                size_t offset = mem_offset(n_fields, fields, tmp->info.n_rows, 0);
                memcpy(tmp->data + offset, table_a->data + table_a_offset, row_size(table_a->info));
                memcpy(tmp->data + offset + row_size(table_a->info), table_b->data + table_b_offset,
                       row_size(table_b->info));
                tmp->info.n_rows++;
            }
        }
    }

    return tmp;
}

#ifdef DEBUG

void print_table(table *t) {
    char text[MAX_FIELD_LENGTH];
    for (int i = 0; i < t->info.n_rows; i++) {
        for (int j = 0; j < t->info.n_fields; j++) {
            decode_temp_table_field(text, t, i, j);
            printf("%12s ", text);
        }
        printf("\n");
    }
}
#endif

bool do_join_query(query q) {
    int rs_size = 1;

    // we ignore outer joins

    result_set **rs_c = alloca(sizeof(result_set *) * q.n_tables);
    for (int i = 0; i < q.n_tables; i++) {
        rs_size *= q.tables[i]->info.n_rows;
        rs_c[i] = create_result_set(q.tables[i]);
    }

    // run all filters
    int filter_cond_count = 0;
    for (int i = 0; i < q.n_conditions; i++) {
        if (q.conditions[i].literal2.type != literal_type_constant) {
            continue;
        }
        filter_cond_count++;
        result_set *rs = get_result_set(rs_c, rs_size, q.conditions[i].literal1.table);
        filter(rs->include_rows, q.conditions[i].literal1.table, q.conditions[i].literal1.col,
               q.conditions[i].literal2.value);
    }

//    join_result_set **jrs_c = alloca(sizeof(join_result_set *) * q.n_conditions - filter_cond_count);
    table *result = NULL;
    int index = 0;
    for (int i = 0; i < q.n_conditions; i++) {
        if (!q.conditions[i].literal2.type != literal_type_constant) {
            continue;
        }

        if (index == 0) {
            result_set *rs = get_result_set(rs_c, rs_size, q.conditions->literal1.table);
            result = table_to_temp_table(q.conditions[i].literal1.table, rs->include_rows);
#ifdef DEBUG
            print_table(result);
#endif
        }

        table *tmp = table_to_temp_table(q.conditions[i].literal2.table,
                                         get_result_set(rs_c, rs_size,
                                                        q.conditions->literal2.table)->include_rows);

        result = temp_table_inner_join(result, table_find_field(result->info, q.conditions[i].literal1.value), tmp,
                                       q.conditions[i].literal2.col);
        index++;
    }

#ifdef DEBUG
    printf("Rows: %lu\n", result->info.n_rows);
//    for (int i = 0; i < result->n_fields; i++) {
//        printf("%s\t%s(%lu)\n", result->fields[i].name, field_type_to_str(result->fields[i].type),
//               result->fields[i].length);
//    }
    puts("-------");
#endif

    int field_names[q.n_fields];
    for (int k = 0; k < q.n_fields; k++) {
        field_names[k] = table_find_field(result->info, q.fields[k].field);
    }

    char value[MAX_FIELD_LENGTH];
    for (size_t i = 0; i < result->info.n_rows; i++) {
        for (int j = 0; j < q.n_fields; j++) {
            decode_temp_table_field(value, result, i, field_names[j]);
            printf("%s%s", j == 0 ? "" : ",", value);
        }
        printf("\n");
    }

    return true;
}

table *open_index(const char *name) {
    table *t = malloc(sizeof(table));
    t->temporary = true;
    char filename[FILENAME_MAX];
    sprintf(filename, "%s.index", name);
    FILE *fp = fopen(filename, "rb");
    fread(&t->info, sizeof(table_info), 1, fp);

    if (ferror(fp)) {
        free(t);
        return NULL;
    }

    fclose(fp);

    sprintf(filename, "%s.index.bin", name);
    fp = fopen(filename, "rb");

    size_t size = row_size(t->info) * t->info.n_rows * sizeof(uint8_t);
    t->data = malloc(size);

    fread(t->data, size, 1, fp);

    if (ferror(fp)) {
        free(t->data);
        free(t);
        return NULL;
    }

    fclose(fp);
    return t;
}

bool parse_select(const char *input) {
    // parse select fields
    char buf[INPUT_BUFFER_SIZE];

    char operator[8];

    query q;
    q.n_fields = 0;
    q.n_tables = 0;
    q.n_conditions = 0;

    char fields[SELECT_MAX][MAX_FIELD_NAME_SIZE] = {0};

    if (sscanf(input, "SELECT %[^\n]%*c", buf) == 1) {

        char *tok = strtok(buf, ",");
        while (tok != NULL) {
            strcpy(fields[q.n_fields++], str_trim(tok));
            tok = strtok(NULL, ",");
        }

//#ifdef DEBUG
//        for (int i = 0; i < q.n_fields; i++) {
//            printf("F: %s \n", fields[i]);
//        }
//#endif

        do {
            fgets(buf, INPUT_BUFFER_SIZE, stdin);
            str_trim(buf);

#ifndef QUIET
            printf("===> %s\n", buf);
#endif


            if (starts_with("FROM", buf)) {
                strtok(buf, " ");
                while ((tok = strtok(NULL, ","))) {
                    //strcpy(q.tables[q.n_tables++], str_trim(tok));
                    table *t = open_table(str_trim(tok));
                    if (t == NULL) {
                        t = open_index(tok);
                    }
                    if (NULL != t) {
                        q.tables[q.n_tables++] = t;
                    } else {
                        fputs("Table does not exist", stderr);
                        return false;
                    }
                }

            } else if (starts_with("WHERE", buf) || starts_with("AND", buf) || starts_with("OR", buf)) {
                char op1[MAX_FIELD_NAME_SIZE];
                char op2[MAX_FIELD_NAME_SIZE];
                char conj[32];
                sscanf(buf, "%s %s %s %s", conj, op1, operator, op2);
                int index = q.n_conditions;

                parse_query_literal(&q.conditions[index].literal1, op1, q.tables, q.n_tables);
                parse_query_literal(&q.conditions[index].literal2, op2, q.tables, q.n_tables);
                parse_query_operator(&q.conditions[index].operator, operator);

                if (q.conditions[index].literal1.type == literal_type_constant &&
                    q.conditions[index].literal2.type == literal_type_field) {
                    literal temp = q.conditions[index].literal1;
                    q.conditions[index].literal1 = q.conditions[index].literal2;
                    q.conditions[index].literal2 = temp;
                }

                if (strcmp(conj, "WHERE") == 0 || strcmp(conj, "AND") == 0) {
                    q.conditions[index].conjunction = conjunction_and;
                } else if (strcmp(conj, "OR") == 0) {
                    q.conditions[index].conjunction = conjunction_or;
                }
                q.n_conditions++;
            } else if (starts_with("END", buf)) {
                break;
            }

        } while (!feof(stdin));

        // normalize field names
        for (int i = 0; i < q.n_fields; i++) {
            bool found = false;
            for (int j = 0; j < q.n_tables; j++) {
                int col = table_find_field(q.tables[j]->info, fields[i]);
                if (col != -1) {
                    strcpy(q.fields[i].table, q.tables[j]->info.name);
                    strcpy(q.fields[i].field, fields[i]);
                    q.fields[i].col = col;
                    found = true;
                    break;
                }
            }

            if (!found) {
                return false;
            }
        }


        if (1 == q.n_tables && !has_self_join(q)) {
            return single_query(q);
        } else {
            return do_join_query(q);
        }
        return true;
    }

    return false;
}

void parse_drop(const char *input) {
    puts("DROP");
}

bool show_table_info(const table_info t) {
    printf("Table: %s\n", t.name);
    printf("Row size: %lu\n", row_size(t));
    for (int i = 0; i < t.n_fields; i++) {
        printf("%s\t%s(%lu)\n", t.fields[i].name, field_type_to_str(t.fields[i].type), t.fields[i].length);
    }
    return true;
}

bool show_table(const char *table_name) {
    table_info t;
    if (read_table_info(&t, table_name)) {
        return show_table_info(t);
    }
    return false;
}

bool parse_show_table(const char *input) {
    char table_name[MAX_TABLE_NAME_SIZE];
    int n = sscanf(input, "SHOW %s", table_name);
    if (n != 1) {
        return false;
    }

    return show_table(table_name);
}

int strcmp_wrapper(const void *a, const void *b) {
#ifdef DEBUG
    printf("Comparing %s %s\n", (const char *) a, (const char *) b);
#endif
    return strcmp((const char *) a, (const char *) b);
}

bool parse_create_index(const char *input) {
    char table_name[MAX_TABLE_NAME_SIZE];
    char field_names[INPUT_BUFFER_SIZE];
    char index_name[MAX_TABLE_NAME_SIZE];
    char buf[INPUT_BUFFER_SIZE];

    int n = sscanf(input, "CREATE INDEX %s USING %[^\n]%*c", index_name, field_names);
    if (n != 2) {
        return false;
    }
    fgets(buf, INPUT_BUFFER_SIZE, stdin);
    n = sscanf(str_trim(buf), "FROM %s", table_name);
    if (n != 1) {
        return false;
    }
#ifndef QUIET
    printf("===> %s\n",buf);
#endif
    fgets(buf, INPUT_BUFFER_SIZE, stdin);
    if (strcmp("END", str_trim(buf)) != 0) {
        fprintf(stderr, "Unknown command: %s\n", buf);
    }
#ifndef QUIET
    printf("===> %s\n",buf);
#endif
    table *t = open_table(table_name);

    if (t == NULL) {
        fputs("Table not found", stderr);
        return false;
    }

    int cols[MAX_TABLE_FIELDS];
    field fields[MAX_TABLE_FIELDS];
    int n_cols = 0;
    char *tok = strtok(field_names, ",");
    while (tok != NULL) {
        int col = table_find_field(t->info, str_trim(tok));
        if (col != -1) {
            cols[n_cols] = col;
            fields[n_cols++] = t->info.fields[col];
        } else {
            return false;
        }
        tok = strtok(NULL, ",");
    }

    table *tmp = create_temp_table(n_cols, fields, t->info.n_rows);
    tmp->info.n_rows = t->info.n_rows;
    uint8_t value[MAX_FIELD_LENGTH];
    for (int i = 0; i < tmp->info.n_rows; i++) {
        for (int j = 0; j < n_cols; j++) {
            read_field(value, t, i, cols[j]);
            set_temp_table_field(tmp, i, j, value);
        }
    }

    qsort(tmp->data, tmp->info.n_rows, row_size(tmp->info), strcmp_wrapper);

    char filename[FILENAME_MAX];
    sprintf(filename, "%s.index", index_name);
    FILE *fp = fopen(filename, "wb");
    fwrite(&tmp->info, sizeof(table_info), 1, fp);
    fclose(fp);

    sprintf(filename, "%s.index.bin", index_name);
    fp = fopen(filename, "wb");
    fwrite(tmp->data, row_size(tmp->info) * tmp->info.n_rows * sizeof(uint8_t), 1, fp);
    fclose(fp);

    //close_temp_table(tmp);

#ifdef DEBUG
    print_table(tmp);
#endif

    return true;
}

bool parse_input(const char *input) {
    if (starts_with("CREATE TABLE", input)) {
        return parse_create(input);
    } else if (starts_with("INSERT", input)) {
        return parse_insert(input);
    } else if (starts_with("DELETE", input)) {
        return parse_delete(input);
    } else if (starts_with("SELECT", input)) {
        return parse_select(input);
    } else if (starts_with("DROP", input)) {
        //parse_drop(input);
        return true;
    } else if (starts_with("SHOW", input)) {
        return parse_show_table(input);
    } else if (starts_with("CREATE INDEX", input)) {
        return parse_create_index(input);
    } else if (strcmp(input, "QUIT") == 0) {
        puts("Bye");
        return true;
    }
#ifdef DEBUG
    fprintf(stderr, "Unknown command: %s\n", input);
#endif
    //puts(input);
    return false;
}

int main() {
    char input[INPUT_BUFFER_SIZE];

#ifndef QUIET
    puts("Welcome!");
#endif
    do {
        input[0] = 0;
        fgets(input, INPUT_BUFFER_SIZE, stdin);
        str_trim(input);
        if(strlen(input) < 2) {
            continue;
        }

#ifndef QUIET
        printf("===> %s\n", input);
#endif
        if (starts_with("QUIT", input)) {
            break;
        } else {
            parse_input(input);
        }
    } while (!feof(stdin));

#ifndef QUIET
    puts("Goodbye!");
#endif
    return 0;
}