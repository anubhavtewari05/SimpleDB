#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>
#include<stdint.h>
#include<fcntl.h>
#include<sys/stat.h>
#include<windows.h>


#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum{
    STATEMENT_INSERT, 
    STATEMENT_SELECT
} StatementType;

typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1 ]; // + 1 for NUll termination 
    char email[COLUMN_EMAIL_SIZE + 1 ];
} Row;

typedef struct{
    StatementType type;
    Row row_to_insert; 
} Statement;

struct Connection_t {
  int file_descriptor;
};
typedef struct Connection_t Connection;

Connection* open_connection(char* filename) {
    int fd = open(
            filename,
            O_RDWR | O_CREAT,       // Read/write, create if not exists
            S_IREAD       // Owner: read permission
);

  if (fd == -1) {
    printf("Unable to open file '%s'\n", filename);
    exit(EXIT_FAILURE);
  }

  Connection* connection = malloc(sizeof(Connection));
  connection->file_descriptor = fd;

  return connection;
}

char* get_db_filename(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a filename for the database.\n");
    exit(EXIT_FAILURE);
  }
  return argv[1];
}

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

/* Sizes of each column */
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

/* Offsets and total row size — must be compile-time constants */
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE sizeof(Row)

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096; // 4KB
#define ROWS_PER_PAGE (PAGE_SIZE/ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)


typedef struct {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} Buffer;

typedef struct
{
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
}Table;

void print_row(Row* row){
    printf("(%d, %s , %s)\n",row->id,row->username,row->email);
}

void serialize_row(Row* source, void* destination) {
    memcpy((char*)destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy((char*)destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy((char*)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), (char*)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), (char*)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (char*)source + EMAIL_OFFSET, EMAIL_SIZE);
}


void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;

    void* page = table->pages[page_num];
    if (page == NULL) {
        page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        table->pages[page_num] = page;
    }

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return (void *)((uint8_t*)page + byte_offset);
}


/*void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num/ ROWS_PER_PAGE;
    void* page = table->pages[page_num];

    if(page==NULL){
        //Allocate mem only when we try to access page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}*/

Table* new_table(){
    Table* table = (Table*)malloc(sizeof(Table));
    table -> num_rows = 0;
    for(uint32_t i=0; i<TABLE_MAX_PAGES;i++)
    {
        table->pages[i] = NULL;
    }
    return table;
}

void free_table(Table *table)
{
    for(int i = 0; table->pages[i];i++)
    {
        free(table->pages[i]);
    }
    free(table);
}

Buffer* new_input_buffer(){
    Buffer* ipbuffer = malloc(sizeof(Buffer));
    ipbuffer->buffer = NULL;
    ipbuffer->buffer_length = 0;
    ipbuffer->input_length=0;

    return ipbuffer;
} /*Buffer Allocation */

void close_input_buffer(Buffer* ipbuffer) {
    free(ipbuffer->buffer);
    free(ipbuffer);
} /*Buffer De-allocation*/

MetaCommandResult do_meta_command(Buffer* ipbuffer){
    if (strcmp(ipbuffer->buffer, ".exit") == 0) {
        close_input_buffer(ipbuffer);
        exit(EXIT_SUCCESS);
    }
    else {
        return META_COMMAND_UNRECOGNIZED;
    }
} // TO Check and return if the ip is a Meta command and if its successfully executed

PrepareResult prepare_statement(Buffer* ipbuffer, Statement* statement){
    if (strncmp(ipbuffer->buffer, "insert",6) == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(ipbuffer->buffer,"insert %d %31s %255s", &(statement->row_to_insert.id),statement->row_to_insert.username,statement->row_to_insert.email);
        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        printf("prepare: id=%u username='%s' email='%s'\n",
                statement->row_to_insert.id,
                statement->row_to_insert.username,
                statement->row_to_insert.email);
        return PREPARE_SUCCESS;
    }
    if (strcmp(ipbuffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    if(table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    void* slot = row_slot(table, table->num_rows);
    serialize_row(row_to_insert, slot);
    printf("Inserting id=%u username=%s email=%s into slot %p\n",
       row_to_insert->id, row_to_insert->username, row_to_insert->email, slot);  
    serialize_row(row_to_insert, slot);
    printf("row 0 slot = %p, row 1 slot = %p\n",
       row_slot(table,0), row_slot(table,1));
    table->num_rows += 1 ;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Row row;
    for(uint32_t i=0; i<table->num_rows; i++)
    {
        deserialize_row(row_slot(table,i),&row);
        printf("Reading row %u from slot %p\n", i, row_slot(table, i));  
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch(statement->type){
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

void print_prompt(){
    printf("SimpleDB > ");
    fflush(stdout);  
} /*THe prompt for the DB*/

void read_input(Buffer* ipbuffer) {
    size_t bytes_read = getline(&(ipbuffer->buffer), &(ipbuffer->buffer_length), stdin);
    if(bytes_read == -1) {
        printf("Error Reading Input \n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    ipbuffer->input_length = bytes_read - 1;
    ipbuffer->buffer[bytes_read - 1] = 0;
}

int main(int argc, char* argv[]){
    Table* table = new_table();
    Buffer* ipbuffer = new_input_buffer();
    while(1)
    {
        print_prompt();
        read_input(ipbuffer);

        if(ipbuffer->buffer[0] == '.'){
            switch(do_meta_command(ipbuffer)) {
                case(META_COMMAND_SUCCESS):
                    continue;
                case(META_COMMAND_UNRECOGNIZED):
                    printf("Unrecognized Command '%s'.\n", ipbuffer->buffer);
                    continue;
            }
        }
        Statement statement;
        switch(prepare_statement(ipbuffer,&statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax ERROR. Could not parse");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of %s .\n",ipbuffer->buffer);
                continue;
        }
        switch (execute_statement(&statement, table))
        {
        case (EXECUTE_SUCCESS):
            printf("Executed. \n");
            break;
        case (EXECUTE_TABLE_FULL):
            printf("Error: Table is Full. \n");
            break;
        }
    }
}