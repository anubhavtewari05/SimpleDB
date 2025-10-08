#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>
#include<stdint.h>


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
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct{
    StatementType type;
    Row row_to_insert; 
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

/* Sizes of each column */
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

/* Offsets and total row size — must be compile-time constants */
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096; // 4KB
#define ROWS_PER_PAGE PAGE_SIZE/ROW_SIZE
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
    uint8_t* dest = (uint8_t*) destination;  // cast for byte-wise arithmetic

    memcpy(dest + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    uint8_t* src = (uint8_t*) source;  // cast for byte-wise arithmetic

    memcpy(&(destination->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;

    /* sanity check: ensure pages array exists and page_num is in range */
    if (table == NULL || table->pages == NULL) {
        fprintf(stderr, "row_slot: table or table->pages is NULL\n");
        return NULL;
    }
    if (page_num >= TABLE_MAX_PAGES) { /* define TABLE_MAX_PAGES or use table->num_pages */
        fprintf(stderr, "row_slot: page_num %u out of range (max %u)\n",
                page_num, (unsigned)TABLE_MAX_PAGES);
        return NULL;
    }

    void* page = table->pages[page_num];
    if (page == NULL) {
        page = malloc(PAGE_SIZE);
        if (page == NULL) {
            perror("malloc PAGE_SIZE");
            return NULL;
        }
        memset(page, 0, PAGE_SIZE);
        table->pages[page_num] = page;
    }

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    /* make sure we won't walk off the page */
    if (byte_offset + ROW_SIZE > PAGE_SIZE) {
        fprintf(stderr, "row_slot: computed offset out of page bounds\n");
        return NULL;
    }

    /* IMPORTANT: cast to a byte pointer for correct pointer arithmetic */
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
        int args_assigned = sscanf(ipbuffer->buffer,"insert %d %s %s", &(statement->row_to_insert.id),statement->row_to_insert.username,statement->row_to_insert.email);
        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
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

    serialize_row(row_to_insert, row_slot(table,table->num_rows));
    table->num_rows += 1 ;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Row row;
    for(uint32_t i=0; i< table->num_rows; i++)
    {
        deserialize_row(row_slot(table,i),&row);
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
} /*THe prompt for the DB*/

void read_input(Buffer* ipbuffer){
    size_t bytes_read = getline(&(ipbuffer->buffer), &(ipbuffer->buffer_length),stdin);
    if(bytes_read <=0) {
        printf("Error Reading Input \n");
        exit(EXIT_FAILURE);
    }
    
    //Ignore trailing newline 
    ipbuffer->input_length = bytes_read-1;
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