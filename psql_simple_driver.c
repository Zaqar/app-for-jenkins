#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>


#define LOGON_OPTIONS_LENGTH 8 * sizeof(char *)
#define MAX_DB_OBJECT_LENGTH 128

#define CHAROID 18
#define BPCHAROID 1042
#define VARCHAROID 1043


typedef enum
{
    HOST = 0,
    PORT,
    USER,
    PASS,
    DB_NAME,
    APP_NAME,
    ENCODING,
    LSN,
    TERMINATOR
} LOGON_OPTIONS;


PGconn *connection;

bool
connect_to_db(void) {
    char *logon_keywords[LOGON_OPTIONS_LENGTH] = {0};
    logon_keywords[HOST] = "host";
    logon_keywords[PORT] = "port";
    logon_keywords[USER] = "user";
    logon_keywords[DB_NAME] = "dbname";
    logon_keywords[PASS] = "password";
    logon_keywords[APP_NAME] = "fallback_application_name";
    logon_keywords[ENCODING] = "client_encoding";

    char user[MAX_DB_OBJECT_LENGTH] = "postgres";
    char host[MAX_DB_OBJECT_LENGTH] = "localhost";
    char port[MAX_DB_OBJECT_LENGTH] = "5432";
    char pass[MAX_DB_OBJECT_LENGTH] = "postgres";
    char db_name[MAX_DB_OBJECT_LENGTH] = "postgres";
    char progname[MAX_DB_OBJECT_LENGTH] = "psql_clinet";

    const char *values[LOGON_OPTIONS_LENGTH] = {0};
    values[HOST] = host;
    values[PORT] = port;
    values[USER] = user;
    values[PASS] = pass;
    values[DB_NAME] = db_name;
    values[APP_NAME] = progname;
    values[ENCODING] = "auto";
    values[LSN] = "0";
    values[TERMINATOR] = NULL;


    connection = PQconnectdbParams((const char * const*)logon_keywords, values, true);
    if (PQstatus(connection) == CONNECTION_BAD) {
        printf("Bad connection\n");
        return false;
    }

    return true;
}


bool
handle_result(const PGresult *result)
{
    bool        OK;

    if (!result)
        OK = false;
    else
        switch (PQresultStatus(result))
        {
            case PGRES_COMMAND_OK:
            case PGRES_TUPLES_OK:
            case PGRES_EMPTY_QUERY:
            case PGRES_COPY_IN:
            case PGRES_COPY_OUT:
                /* Fine, do nothing */
                OK = true;
                break;

            case PGRES_BAD_RESPONSE:
            case PGRES_NONFATAL_ERROR:
            case PGRES_FATAL_ERROR:
                OK = false;
                break;

            default:
                OK = false;
                printf("unexpected PQresultStatus: %d\n", PQresultStatus(result));
                break;
        }

    if (!OK) {
        const char *error = PQerrorMessage(connection);
        if (error) {
            printf("%s\n", error);
        }
    }

    return OK;
}


int
get_type_max_size(const PGresult *result, int column) {
    int size = 0;
    Oid type = PQftype(result, column);
    switch (type) {
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID: {
            size = PQfmod(result, column) - 4;
            break;
        }
        default:
            size = PQfsize(result, column);
    }
    return size;
}

void
print_row(PGresult *result, bool print_header, int columns_count, int row_index) {
    char *row = NULL;
    size_t row_len = 0;
    size_t alloctted_len = 0;
    for (int j = 0; j < columns_count; ++j) {
        char *col_data = NULL;
        int col_width = get_type_max_size(result, j);
        if (print_header) {
            col_data = PQfname(result, j);
        } else {
            col_data = PQgetvalue(result, row_index, j);
        }

        size_t size = snprintf(NULL, 0, "%s", col_data);
        size_t spaces = (int)(col_width - strlen(col_data));
        size_t separator = 2;
        size_t new_size = size + spaces + separator;
        if (row_len + new_size > alloctted_len) {
            row = (char *)realloc(row, row_len + new_size);
            alloctted_len = row_len + new_size;
            memset(&row[row_len], 0, new_size);
        }

        sprintf(&row[row_len], "%s", col_data);
        row_len += size;
        memset(&row[row_len], ' ', spaces);
        row_len += spaces;
        memset(&row[row_len], '|', separator);
        row_len += separator;
    }

    printf("%s\n", row);
    if (print_header) {
        memset(row, '-', row_len);
        printf("%s\n", row);
    }

    free(row);
}

void
print_table_content(PGresult *result, int rows_count) {
    if (result == NULL) {
        return;
    }

    int columns_count = PQnfields(result);
    print_row(result, true, columns_count, 0);
    for (int i = 0; i < rows_count; ++i) {
        print_row(result, false, columns_count, i);
    }
}


void
exec_query(const char *query) {
    if (query == NULL) {
        return;
    }

    char *cmdStatus = NULL;
    PGresult *result = PQexec(connection, query);
    ExecStatusType status = PQresultStatus(result);
    printf("Stmt: %s\n", query);

    if (handle_result(result)) {
        printf("The query was executed successfully\n\n");
    } else {
        PQclear(result);
        printf("Failed while executing the query\n");
        return;
    }

    if (strncasecmp(query, "SELECT", strlen("SELECT")) == 0) {
        char *rows = "rows";
        char *tuples = PQcmdTuples((PGresult *)result);
        int row_count = strtol(tuples, NULL, 10);
        print_table_content(result, row_count);
        if (row_count == 1) {
            rows = "row";
        }

        printf("(%d %s)\n", row_count, rows);
    } else {
        cmdStatus = PQcmdStatus(result);
        if (cmdStatus != NULL) {
            printf("%s\n", PQcmdStatus(result));
        }
    }

    PQclear(result);
}



int main() {
    PGresult *results = NULL;
    bool is_valid_connection = connect_to_db();
    if (is_valid_connection) {
        printf("Connect to db\n");
    }

    exec_query("drop table my_table");
    exec_query("create table my_table (id int, fname varchar(25), sname varchar(30), address char(20), age int, weight float);");
    exec_query("insert into my_table values(1, 'name1', 'sname1', 'address1', 50, 78.5);");
    exec_query("insert into my_table values(2, 'name2', 'sname2', 'address2', 60, 80.5);");
    exec_query("insert into my_table values(3, 'name3', 'sname3', 'address3', 70, 82.5);");
    exec_query("select * from my_table;");
    return 0;
}