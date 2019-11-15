#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include <omp.h>

int validate(const char ** files, int files_size, char ** errors, int errors_size)
{
  int i;

  char cwd[1000];
  if(getcwd(cwd, sizeof(cwd)) != NULL){
    printf("Validating in dir: %s\n", cwd);
  }else{
    perror("getcwd() error");
    return 1;
  }

  for(i = 0; i < files_size; ++i){
    printf("%s\n", files[i]);
  }

  for(i = 0; i < errors_size; ++i){
    errors[i] = "a";
  }

  return 0;
}

// int release(char ** errors){
//   int i;
//   for(i = 0; i < 10; ++i)
//     free(errors[i]);
//   free(errors);
//   return 0;
// }

//gcc -shared -Wl,-install_name,testlib.so -o testlib.so -fPIC testlib.c
