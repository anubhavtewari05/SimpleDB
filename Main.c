#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>
#include<stdint.h>
#include<fcntl.h>
#include<sys/stat.h>
#include<windows.h>
#include<errno.h>
#include<fcntl.h>

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
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
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
  Table* table;
  uint32_t row_num;
  bool end_of_table;  // Indicates a position one past the last element
} Cursor;

typedef struct {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} Buffer;

typedef struct{
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;
//Cache + Access 

typedef struct
{
    uint32_t num_rows;
    Pager* pager;
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
Cursor* table_start(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = 0;
  cursor->end_of_table = (table->num_rows == 0);

  return cursor;
}

Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = table->num_rows;
  cursor->end_of_table = true;

  return cursor;
}

void* get_page(Pager* pager, uint32_t page_num)
{
    if(page_num > TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds. %d > %d\n",page_num,TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if(pager->pages[page_num] == NULL) {
        //Cache miss
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if(pager->file_length % PAGE_SIZE)
        {
            num_pages += 1 ;
        }

        if (page_num<= num_pages)
        {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            size_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1){
                printf("Error reading file: %d\n" , errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

void db_close(Table* table) {
  Pager* pager = table->pager;
  uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
  if (num_additional_rows > 0) {
    uint32_t page_num = num_full_pages;
    if (pager->pages[page_num] != NULL) {
      pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
      free(pager->pages[page_num]);
      pager->pages[page_num] = NULL;
    }
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

void* cursor_value(Cursor* cursor) {
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(cursor->table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return (void *)((uint8_t*)page + byte_offset);
}

void cursor_advance(Cursor* cursor) {
  cursor->row_num += 1;
  if (cursor->row_num >= cursor->table->num_rows) {
    cursor->end_of_table = true;
  }
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

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  size_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], size);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Pager* pager_open(const char* filename)
{
    int fd = open(
            filename,
            O_RDWR | O_CREAT,       // Read/write, create if not exists
            S_IREAD       // Owner: read permission
        );

  if (fd == -1) {
    printf("Unable to open file '%s'\n", filename);
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd,0,SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }
  return pager;
}


Table* db_open(const char* filename){
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;
    
    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
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

MetaCommandResult do_meta_command(Buffer* ipbuffer, Table* table){
    if (strcmp(ipbuffer->buffer, ".exit") == 0) {
        close_input_buffer(ipbuffer);
        exit(EXIT_SUCCESS);
    }
    else {
        return META_COMMAND_UNRECOGNIZED;
    }
} // TO Check and return if the ip is a Meta command and if its successfully executed

PrepareResult prepare_insert(Buffer* ipbuffer, Statement* statement)
{
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(ipbuffer->buffer," ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL," ");
    char* email = strtok(NULL, " ");

    if(id_string == NULL || username == NULL || email == NULL || keyword ==
    "insert")
    {
        return PREPARE_SYNTAX_ERROR;       
    }

    int id = atoi(id_string);
    if(id<0)
    {
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    printf("prepare: id=%u username='%s' email='%s'\n",
                statement->row_to_insert.id,
                statement->row_to_insert.username,
                statement->row_to_insert.email);
    return PREPARE_SUCCESS;
}


PrepareResult prepare_statement(Buffer* ipbuffer, Statement* statement){
    if (strncmp(ipbuffer->buffer, "insert",6) == 0) {
        return prepare_insert(ipbuffer, statement);
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
    Cursor* cursor = table_end(table);
    void* slot = row_slot(table, table->num_rows);
    serialize_row(row_to_insert, cursor_value(cursor));
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
    Cursor* cursor = table_start(table);
    while(!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        printf("Reading row from slot %p\n",row_slot(table));  
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
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
    if(argc < 2) {
        printf("Must supply a database filename. \n");
        exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table* table = db_open(filename);
    Buffer* ipbuffer = new_input_buffer();
    while(1)
    {
        print_prompt();
        read_input(ipbuffer);

        if(ipbuffer->buffer[0] == '.'){
            switch(do_meta_command(ipbuffer,table)) {
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
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive. \n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long. \n");
                continue;
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