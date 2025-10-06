#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>

typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum{
    STATEMENT_INSERT, 
    STATEMENT_SELECT
} StatementType;

typedef struct{
    StatementType type; 
} Statement;

typedef struct {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} Buffer;

Buffer* new_input_buffer(){
    Buffer* ipbuffer = malloc(sizeof(Buffer));
    ipbuffer->buffer = NULL;
    ipbuffer->buffer_length = 0;
    ipbuffer->input_length=0;
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
    if (strcmp(ipbuffer->buffer, "insert",6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strcmp(ipbuffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement){
    switch(statement->type){
        case (STATEMENT_INSERT):
            printf("This is where INSERT operation will be done ");
            break;
        case (STATEMENT_SELECT):
            printf("This is where SELECT operation will be done");
            break; 
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
                    printf("Unrecognized Command %s \n", ipbuffer);
                    continue;
            }
        }
        Statement statement;
        switch(prepare_statement(ipbuffer,&statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of %s .\n",ipbuffer->buffer);
                continue;
        }
        execute_statement(&statement);
        printf("Executed.\n");
    }
}