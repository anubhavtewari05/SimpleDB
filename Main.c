#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>

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

void close_input_buffer(Buffer* ipbuffer) {
    free(ipbuffer->buffer);
    free(ipbuffer);
}

int main(int argc, char* argv[]){
    Buffer* ipbuffer = new_input_buffer();
    while(1)
    {
        print_prompt();
        read_input(ipbuffer);

        if(strcmp(ipbuffer->buffer,".exit")==0){
            close_input_buffer(ipbuffer);
            exit(EXIT_SUCCESS);
        }
        else{
            printf("Unrecognized command '%s'.\n", ipbuffer->buffer);
        }
    }
    return 0;
}