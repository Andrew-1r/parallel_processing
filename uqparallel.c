#include <csse2310a3.h>
#include <ctype.h>
#include <fcntl.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

const char *const jobLimit = "--joblimit";
const char *const pipeOption = "--pipe";
const char *const argsFile = "--argsfile";
const char *const dryRun = "--dry-run";
const char *const exitOnError = "--exit-on-error";
const char *const perTask = ":::";
const char stdoutFile = '>';
const char *const stderrFile = "2>";
const char *const usageErrorMessage =
    "Usage: ./uqparallel [--pipe] [--exit-on-error] [--joblimit n] "
    "[--dry-run] [--argsfile argument-file] [cmd [fixed-args ...]] [::: "
    "per-task-args ...]\n";

#define JOB_LIMIT_MIN 1
#define JOB_LIMIT_MAX 120
#define JOB_LIMIT_DEFAULT 120
#define USAGE_ERROR_EXIT_NUM 10
#define FILE_READ_ERROR_EXIT_NUM 18
#define EMPTY_COMMAND_EXIT_NUM 94
#define SIGNAL_EXIT_NUM 78
#define PER_TASK_LENGTH 3
#define STDERRFILE_LENGTH 2
#define OPTION_HEADER_LENGTH 2
#define STDOUT_FILE_HEADER_LENGTH 1
#define STDERR_FILE_HEADER_LENGTH 2
#define LINE_BUFFER 1024
#define COMMAND 1
#define NULL_TERMINATOR 1
#define PER_TASK_ARG 1
#define FILE_ARGS 0
#define STDIN_ARG 1
#define READ_WRITE_PERMISSIONS 0600

// Structure which contains given command line arguments, aka CLArgs
struct CLArgs {
  bool dryRunPresent;
  bool pipePresent;
  bool exitOnErrorPresent;
  bool jobLimitPresent;
  int jobLimit;

  bool argsFilePresent;
  char *fileName;
  int numFileArgs;
  char **fileArgs;

  bool commandPresent;
  char *command;
  int numFixedArgs;
  char **fixedArgs;

  bool perTaskPresent;
  int numPerTaskArgs;
  char **perTaskArgs;
};

// Structure which contains argments for processing, aka PArgs
struct PArgs {
  char ***args;
  int numArgs;
  int *numElements;
  int stdinArgsPosition;
  char **stdoutFiles;
  char **stderrFiles;
};

// Fills in per-task arguments in a CLArgs struct from argv[]
// Inputs: cmdLineArgs - pointer to CLArgs struct to populate
//         argc - number of per-task args
//         argv - array of per-task arguments
void per_task_arg_struct_helper(struct CLArgs *cmdLineArgs, int argc,
                                char *argv[]) {
  if (argc == 0) {
    return;
  }

  cmdLineArgs->numPerTaskArgs = argc;

  cmdLineArgs->perTaskArgs =
      (char **)malloc(cmdLineArgs->numPerTaskArgs * sizeof(char *));

  for (int i = 0; i < argc; i++) {
    cmdLineArgs->perTaskArgs[i] = strdup(argv[i]);
  }
}

// Fills command and fixed arguments in CLArgs; delegates to
// per_task_arg_struct_helper if perTask is present Inputs: cmdLineArgs -
// pointer to CLArgs struct to populate
//         argc - number of arguments
//         argv - array of arguments
void command_struct_helper(struct CLArgs *cmdLineArgs, int argc, char *argv[]) {
  cmdLineArgs->command = strdup(argv[0]);

  if (argc == 1) {
    return;
  }

  int perTaskPosition = 0;
  int fixedArgCount = 0;

  // look for arguments or perTask
  for (int i = 1; i < argc; i++) {
    // If perTask is found, record for later use and break out of loop
    if (strcmp(argv[i], perTask) == 0) {
      cmdLineArgs->perTaskPresent = true;
      perTaskPosition = i;
      break;
    }
    fixedArgCount += 1;
  }

  // Update the number of fixed args that were found
  cmdLineArgs->numFixedArgs = fixedArgCount;

  // Dynamically allocate fixed args
  if (cmdLineArgs->numFixedArgs > 0) {
    cmdLineArgs->fixedArgs =
        (char **)malloc(cmdLineArgs->numFixedArgs * sizeof(char *));
    for (int j = 0; j < cmdLineArgs->numFixedArgs; j++) {
      cmdLineArgs->fixedArgs[j] = strdup(argv[j + 1]);
    }
  }

  if ((cmdLineArgs->perTaskPresent) && (perTaskPosition < argc)) {
    per_task_arg_struct_helper(cmdLineArgs, argc - (perTaskPosition + 1),
                               argv + perTaskPosition + 1);
  }
}

// Modifies input line by trimming unnecessary spaces and preserving quoted
// substrings Inputs: line - original input line Returns: processed copy of the
// string, caller must free
char *modify_string(char *line) {
  bool insideQuotes = false;
  char *processedLine = malloc(strlen(line) + 1);
  int read = 0;
  int write = 0;
  bool spaceAllowed = false;

  while (line[read] != '\0') {
    // If inside quotes write everything
    if (insideQuotes && line[read] != '"') {
      processedLine[write++] = line[read++];
      continue;
    }
    // If inside quotes but have hit another quote, then we are now outside of
    // them
    if (line[read] == '"') {
      processedLine[write++] = line[read++];
      insideQuotes = !insideQuotes;
      if (!insideQuotes) {
        spaceAllowed = true;
      }
      continue;
    }
    if (isblank(line[read])) {
      if (line[read] == ' ' && spaceAllowed) {
        processedLine[write++] = line[read++];
        spaceAllowed = false;
      } else {
        read += 1;
      }
      continue;
    }
    processedLine[write++] = line[read++];
    spaceAllowed = true;
  }

  // remove trailing whitespace and terminate
  if (write > 0 && processedLine[write - 1] == ' ') {
    write--;
  }

  processedLine[write] = '\0';

  return processedLine;
}
// Reads lines from argsfile and populates fileArgs and numFileArgs in CLArgs
// Inputs: cmdLineArgs - CLArgs struct with fileName set
void file_args_struct_helper(struct CLArgs *cmdLineArgs) {
  FILE *file = fopen(cmdLineArgs->fileName, "r");

  char line[LINE_BUFFER];
  int processedLineCount = 0;

  while (fgets(line, LINE_BUFFER, file) != NULL) {
    line[strcspn(line, "\n")] = '\0';  // remove newline
    char *processedLine = modify_string(line);

    char **temp = (char **)realloc((void *)cmdLineArgs->fileArgs,
                                   (processedLineCount + 1) * sizeof(char *));
    cmdLineArgs->fileArgs = temp;

    cmdLineArgs->fileArgs[processedLineCount++] = strdup(processedLine);
    free(processedLine);
  }

  cmdLineArgs->numFileArgs = processedLineCount;
  fclose(file);
}

// Validates that the file in cmdLineArgs->fileName exists and can be opened
// Inputs: cmdLineArgs - CLArgs struct with fileName
// Exits with FILE_READ_ERROR_EXIT_NUM on error
void validate_file(const struct CLArgs *cmdLineArgs) {
  FILE *file = fopen(cmdLineArgs->fileName, "r");
  if (!file) {
    fprintf(stderr, "uqparallel: Cannot open file \"%s\" for reading\n",
            cmdLineArgs->fileName);
    exit(FILE_READ_ERROR_EXIT_NUM);
  }

  fclose(file);
}

// Exits with USAGE_ERROR_EXIT_NUM if alreadyExists is true
// Inputs: alreadyExists - boolean flag indicating prior occurrence
void check_duplicate_option(bool alreadyExists) {
  if (alreadyExists) {
    fprintf(stderr, "%s", usageErrorMessage);
    exit(USAGE_ERROR_EXIT_NUM);
  }
}

// Parses command-line argv and populates CLArgs struct accordingly
// Inputs: argc - number of command-line arguments
//         argv - array of command-line arguments
// Returns: pointer to a newly allocated CLArgs struct
struct CLArgs *command_line_struct_creator(int argc, char *argv[]) {
  struct CLArgs *cmdLineArgs = calloc(1, sizeof(struct CLArgs));

  int i = 0;

  cmdLineArgs->jobLimit = JOB_LIMIT_DEFAULT;

  // check for optional commands and update booleans to true if seen
  for (; i < argc; i++) {
    if (strcmp(argv[i], jobLimit) == 0) {
      check_duplicate_option(cmdLineArgs->jobLimitPresent);
      cmdLineArgs->jobLimitPresent = true;
      cmdLineArgs->jobLimit = atoi(argv[++i]);
    } else if (strcmp(argv[i], pipeOption) == 0) {
      check_duplicate_option(cmdLineArgs->pipePresent);
      cmdLineArgs->pipePresent = true;
    } else if (strcmp(argv[i], argsFile) == 0) {
      check_duplicate_option(cmdLineArgs->argsFilePresent);
      cmdLineArgs->argsFilePresent = true;
      cmdLineArgs->fileName = strdup(argv[++i]);
      validate_file(cmdLineArgs);
      file_args_struct_helper(cmdLineArgs);
    } else if (strcmp(argv[i], dryRun) == 0) {
      check_duplicate_option(cmdLineArgs->dryRunPresent);
      cmdLineArgs->dryRunPresent = true;
    } else if (strcmp(argv[i], exitOnError) == 0) {
      check_duplicate_option(cmdLineArgs->exitOnErrorPresent);
      cmdLineArgs->exitOnErrorPresent = true;
    }
    // optional arguments finished, have hit command or per task
    else {
      break;
    }
  }
  // allocate command and per task handling to helper functions
  if (i < argc) {
    if (strcmp(argv[i], perTask) == 0) {
      cmdLineArgs->perTaskPresent = true;
      per_task_arg_struct_helper(cmdLineArgs, argc - (i + 1), argv + i + 1);
    } else {
      cmdLineArgs->commandPresent = true;
      command_struct_helper(cmdLineArgs, argc - i, argv + i);
    }
  }
  return cmdLineArgs;
}

// Populates pArgs->args for per-task argument usage
// Inputs: cmdLineArgs - filled CLArgs struct
//         pArgs - pointer to PArgs struct to populate
void process_struct_per_task_helper(const struct CLArgs *cmdLineArgs,
                                    struct PArgs *pArgs) {
  // populate pArgs->args with args ready for execvp
  if (cmdLineArgs->numFixedArgs > 0) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] =
          COMMAND + cmdLineArgs->numFixedArgs + PER_TASK_ARG + NULL_TERMINATOR;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;

      pArgs->args[i][0] = strdup(cmdLineArgs->command);

      // populate array with fixed args
      int writePointer = 1;
      for (int j = 0; j < cmdLineArgs->numFixedArgs; j++) {
        pArgs->args[i][writePointer++] = strdup(cmdLineArgs->fixedArgs[j]);
      }

      // place ith per task arg as second last element
      pArgs->args[i][writePointer++] = strdup(cmdLineArgs->perTaskArgs[i]);

      // place null terminator at the end
      pArgs->args[i][writePointer] = NULL;
    }
  } else if (cmdLineArgs->commandPresent) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = COMMAND + PER_TASK_ARG + NULL_TERMINATOR;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;
      pArgs->args[i][0] = strdup(cmdLineArgs->command);
      pArgs->args[i][1] = strdup(cmdLineArgs->perTaskArgs[i]);
      pArgs->args[i][2] = NULL;
    }
  } else {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = PER_TASK_ARG + NULL_TERMINATOR;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;

      pArgs->args[i][0] = strdup(cmdLineArgs->perTaskArgs[i]);
      pArgs->args[i][1] = NULL;
    }
  }
}

// Parses tokens to update args, stdoutFile, and stderrFile for a single task
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
//         tokens - array of tokenized strings
//         numTokens - number of tokens
//         i - index of task
//         writePointer - pointer to current position in args[i]
void file_arg_helper_helper(const struct CLArgs *cmdLineArgs,
                            struct PArgs *pArgs, char **tokens, int numTokens,
                            int i, int *writePointer) {
  // allocate token to args, stdoutFile, or stderrFile depending on what it is
  for (int j = 0; j < numTokens; j++) {
    // if stdoutFile or stderrFile are found, remove a memory space from
    // pArgs->args remember the file and update a boolean if --pipe is not
    // present
    if (tokens[j][0] == stdoutFile) {
      pArgs->numElements[i]--;
      pArgs->args[i] = (char **)realloc((void *)pArgs->args[i],
                                        pArgs->numElements[i] * sizeof(char *));
      if (!cmdLineArgs->pipePresent) {
        free(pArgs->stdoutFiles[i]);
        pArgs->stdoutFiles[i] = strdup(tokens[j] + STDOUT_FILE_HEADER_LENGTH);
      }
    } else if (strncmp(tokens[j], stderrFile, 2) == 0) {
      pArgs->numElements[i]--;
      pArgs->args[i] = (char **)realloc((void *)pArgs->args[i],
                                        pArgs->numElements[i] * sizeof(char *));
      if (!cmdLineArgs->pipePresent) {
        free(pArgs->stderrFiles[i]);
        pArgs->stderrFiles[i] = strdup(tokens[j] + STDERR_FILE_HEADER_LENGTH);
      }
    } else {
      pArgs->args[i][(*writePointer)++] = strdup(tokens[j]);
    }
  }
}

// Handles tokenizing and populating pArgs->args[i] from fileArgs[i]
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
//         writePointer - index to start writing tokens
//         i - index of the task
void file_arg_helper(const struct CLArgs *cmdLineArgs, struct PArgs *pArgs,
                     int writePointer, int i) {
  // tokenise the line arguments
  int numTokens = 0;
  char *line = strdup(cmdLineArgs->fileArgs[i]);
  char **tokens = split_space_not_quote(line, &numTokens);
  pArgs->numElements[i] += numTokens;

  // allocate memory based on how many tokens exist
  char **temp = (char **)realloc((void *)pArgs->args[i],
                                 pArgs->numElements[i] * sizeof(char *));
  pArgs->args[i] = temp;

  file_arg_helper_helper(cmdLineArgs, pArgs, tokens, numTokens, i,
                         &writePointer);

  // was getting double NULL values at the end for some reason, this fixed it
  pArgs->numElements[i] = writePointer + 1;
  char **finalRealloc = (char **)realloc(
      (void *)pArgs->args[i], (pArgs->numElements[i]) * sizeof(char *));
  pArgs->args[i] = finalRealloc;

  // place null terminator at the end
  pArgs->args[i][writePointer] = NULL;

  free((void *)tokens);
  free(line);
}

// Populates PArgs struct when argsfile option is present
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
void process_struct_argsfile_helper(const struct CLArgs *cmdLineArgs,
                                    struct PArgs *pArgs) {
  if (cmdLineArgs->numFixedArgs > 0) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] =
          COMMAND + cmdLineArgs->numFixedArgs + FILE_ARGS + NULL_TERMINATOR;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;

      pArgs->args[i][0] = strdup(cmdLineArgs->command);

      // populate array with fixed args
      int writePointer = 1;
      for (int j = 0; j < cmdLineArgs->numFixedArgs; j++) {
        pArgs->args[i][writePointer++] = strdup(cmdLineArgs->fixedArgs[j]);
      }

      // populate array with tokenised version of current line of file
      file_arg_helper(cmdLineArgs, pArgs, writePointer, i);
    }
  } else if (cmdLineArgs->commandPresent) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = COMMAND + FILE_ARGS + NULL_TERMINATOR;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;
      pArgs->args[i][0] = strdup(cmdLineArgs->command);

      int writePointer = 1;
      file_arg_helper(cmdLineArgs, pArgs, writePointer, i);
    }
  } else {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = COMMAND + FILE_ARGS + NULL_TERMINATOR;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;

      int writePointer = 0;
      file_arg_helper(cmdLineArgs, pArgs, writePointer, i);
    }
  }
}

// Populates PArgs struct in stdin mode, must be NULL terminated outside of this
// function Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
void process_struct_stdin_helper(const struct CLArgs *cmdLineArgs,
                                 struct PArgs *pArgs) {
  if (cmdLineArgs->numFixedArgs > 0) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = COMMAND + cmdLineArgs->numFixedArgs;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;

      pArgs->args[i][0] = strdup(cmdLineArgs->command);

      // populate array with fixed args
      int writePointer = 1;
      for (int j = 0; j < cmdLineArgs->numFixedArgs; j++) {
        pArgs->args[i][writePointer++] = strdup(cmdLineArgs->fixedArgs[j]);
      }

      // make a note of where stdin arg needs to go
      pArgs->stdinArgsPosition = writePointer;
    }
  } else if (cmdLineArgs->commandPresent) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = COMMAND;
      char **temp = (char **)realloc((void *)pArgs->args[i],
                                     (pArgs->numElements[i]) * sizeof(char *));
      pArgs->args[i] = temp;
      pArgs->args[i][0] = strdup(cmdLineArgs->command);

      // make note of position for future stdin args
      pArgs->stdinArgsPosition = 1;
    }
  } else {
    for (int i = 0; i < pArgs->numArgs; i++) {
      pArgs->numElements[i] = 0;
      pArgs->stdinArgsPosition = 0;
    }
  }
}

// Creates and populates a PArgs struct based on mode in CLArgs
// Inputs: cmdLineArgs - pointer to CLArgs struct
// Returns: pointer to a newly allocated PArgs struct
struct PArgs *process_struct_creator(const struct CLArgs *cmdLineArgs) {
  struct PArgs *pArgs = calloc(1, sizeof(struct PArgs));

  // populate pArgs based on per task arguments
  if (cmdLineArgs->perTaskPresent) {
    // Exit if perTask option is present but there are no perTask args
    if (cmdLineArgs->numPerTaskArgs == 0) {
      exit(EMPTY_COMMAND_EXIT_NUM);
    }

    // Allocate memory depending on number of perTask arguments
    pArgs->numArgs = cmdLineArgs->numPerTaskArgs;
    pArgs->args = (char ***)calloc(pArgs->numArgs, sizeof(char **));
    pArgs->numElements = (int *)calloc(pArgs->numArgs, sizeof(int));

    process_struct_per_task_helper(cmdLineArgs, pArgs);
  } else if (cmdLineArgs->argsFilePresent) {
    if (cmdLineArgs->numFileArgs == 0) {
      exit(EMPTY_COMMAND_EXIT_NUM);
    }

    // Allocate memory based on number of lines in the file
    pArgs->numArgs = cmdLineArgs->numFileArgs;
    pArgs->args = (char ***)calloc(pArgs->numArgs, sizeof(char **));
    pArgs->numElements = (int *)calloc(pArgs->numArgs, sizeof(int));
    pArgs->stdoutFiles = (char **)calloc(pArgs->numArgs, sizeof(char *));
    pArgs->stderrFiles = (char **)calloc(pArgs->numArgs, sizeof(char *));

    process_struct_argsfile_helper(cmdLineArgs, pArgs);
    // no argsfile or perTask present
  } else {
    pArgs->numArgs = 1;
    pArgs->args = (char ***)calloc(pArgs->numArgs, sizeof(char **));
    pArgs->numElements = (int *)calloc(pArgs->numArgs, sizeof(int));
    pArgs->stdoutFiles = (char **)calloc(pArgs->numArgs, sizeof(char *));
    pArgs->stderrFiles = (char **)calloc(pArgs->numArgs, sizeof(char *));

    process_struct_stdin_helper(cmdLineArgs, pArgs);
  }

  return pArgs;
}

// Frees all memory allocated in CLArgs struct
// Inputs: cmdLineArgs - pointer to CLArgs struct to free
void free_command_line_struct(struct CLArgs *cmdLineArgs) {
  if (cmdLineArgs->argsFilePresent) {
    free(cmdLineArgs->fileName);
  }

  if (cmdLineArgs->commandPresent) {
    free(cmdLineArgs->command);
  }

  if (cmdLineArgs->numFixedArgs > 0) {
    for (int i = 0; i < cmdLineArgs->numFixedArgs; i++) {
      free(cmdLineArgs->fixedArgs[i]);
    }
    free((void *)cmdLineArgs->fixedArgs);
  }

  if (cmdLineArgs->perTaskPresent) {
    for (int i = 0; i < cmdLineArgs->numPerTaskArgs; i++) {
      free(cmdLineArgs->perTaskArgs[i]);
    }
    free((void *)cmdLineArgs->perTaskArgs);
  }

  if (cmdLineArgs->numFileArgs > 0) {
    for (int i = 0; i < cmdLineArgs->numFileArgs; i++) {
      free(cmdLineArgs->fileArgs[i]);
    }
    free((void *)cmdLineArgs->fileArgs);
  }

  free(cmdLineArgs);
}

// Frees all memory allocated in PArgs struct
// Inputs: pArgs - pointer to PArgs struct to free
void free_process_struct(struct PArgs *pArgs) {
  if (!pArgs) {
    return;
  }

  // Free args if any exist
  if (pArgs->args) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      if (pArgs->args[i]) {
        for (int j = 0; j < pArgs->numElements[i]; j++) {
          free(pArgs->args[i][j]);
        }
        free((void *)pArgs->args[i]);
      }
    }
  }

  if (pArgs->stderrFiles) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      free(pArgs->stderrFiles[i]);
    }
    free((void *)pArgs->stderrFiles);
  }

  if (pArgs->stdoutFiles) {
    for (int i = 0; i < pArgs->numArgs; i++) {
      free(pArgs->stdoutFiles[i]);
    }
    free((void *)pArgs->stdoutFiles);
  }

  free((void *)pArgs->args);
  free(pArgs->numElements);
  free(pArgs);
}

// Creates a single string from an array of strings with spaces in between
// Inputs: array - array of strings
//         numArrayElements - number of elements in array
// Returns: space-separated string, caller must free
char *create_string_from_array(char **array, int numArrayElements) {
  int stringSize = 0;

  // get length of all strings in the given array for initialisation
  for (int i = 0; i < numArrayElements; i++) {
    stringSize += (int)strlen(array[i]);
  }

  // numArrayElements-1 account for spaces and a null terminator
  char *string = malloc(stringSize + numArrayElements);
  int writePointer = 0;

  for (int i = 0; i < numArrayElements; i++) {
    for (size_t j = 0; j < strlen(array[i]); j++) {
      string[writePointer++] = array[i][j];
    }
    if (i != numArrayElements - 1) {
      string[writePointer++] = ' ';
    }
  }
  string[writePointer] = '\0';

  return string;
}

// Performs dry-run printing for per-task mode
// Inputs: cmdLineArgs - pointer to CLArgs struct
void per_task_dry_run(const struct CLArgs *cmdLineArgs) {
  int count = 1;

  for (int i = 0; i < cmdLineArgs->numPerTaskArgs; i++) {
    printf("%i: ", count++);
    if (cmdLineArgs->commandPresent) {
      if (strchr(cmdLineArgs->command, ' ')) {
        printf("\"%s\" ", cmdLineArgs->command);
      } else {
        printf("%s ", cmdLineArgs->command);
      }
    }

    if (cmdLineArgs->numFixedArgs > 0) {
      for (int j = 0; j < cmdLineArgs->numFixedArgs; j++) {
        if (strchr(cmdLineArgs->fixedArgs[j], ' ')) {
          printf("\"%s\" ", cmdLineArgs->fixedArgs[j]);
        } else {
          printf("%s ", cmdLineArgs->fixedArgs[j]);
        }
      }
    }
    if (strchr(cmdLineArgs->perTaskArgs[i], ' ')) {
      printf("\"%s\"\n", cmdLineArgs->perTaskArgs[i]);
    } else {
      printf("%s\n", cmdLineArgs->perTaskArgs[i]);
    }

    fflush(stdout);
  }
}

// Returns true if a line only contains whitespace (excluding backslashes)
// Inputs: line - input string
// Returns: boolean
bool is_blank_line(const char *line) {
  for (int i = 0; line[i] != '\0'; i++) {
    if (line[i] == '\\') {
      // Found a backslash: treat the line as not blank
      return false;
    }
    if (line[i] != ' ' && line[i] != '\t' && line[i] != '\n') {
      // Found any visible character: not blank
      return false;
    }
  }
  // Only spaces, tabs, or newlines
  return true;
}

// Performs dry-run printing for file mode
// Inputs: cmdLineArgs - pointer to CLArgs struct
void file_dry_run(const struct CLArgs *cmdLineArgs) {
  int count = 1;

  for (int i = 0; i < cmdLineArgs->numFileArgs; i++) {
    // only print commands if dry run is present
    if (cmdLineArgs->dryRunPresent) {
      if (cmdLineArgs->numFixedArgs > 0) {
        char *fixedArgString = create_string_from_array(
            cmdLineArgs->fixedArgs, cmdLineArgs->numFixedArgs);
        printf("%i: %s %s %s\n", count, cmdLineArgs->command, fixedArgString,
               cmdLineArgs->fileArgs[i]);
        count += 1;
        free(fixedArgString);
      } else if (cmdLineArgs->commandPresent) {
        printf("%i: %s %s\n", count, cmdLineArgs->command,
               cmdLineArgs->fileArgs[i]);
        count += 1;
      } else {
        if (!(is_blank_line(cmdLineArgs->fileArgs[i]))) {
          printf("%i: %s\n", count, cmdLineArgs->fileArgs[i]);
          count += 1;
        }
      }
      fflush(stdout);
      // why is this here
    } else {
      if (is_blank_line(cmdLineArgs->fileArgs[i])) {
        continue;
      }
    }
  }
}

// Performs dry-run printing for stdin mode
// Inputs: cmdLineArgs - pointer to CLArgs struct
void stdin_dry_run(const struct CLArgs *cmdLineArgs) {
  char line[LINE_BUFFER];
  int count = 1;
  while (fgets(line, sizeof(line), stdin)) {
    char *processedLine = modify_string(line);

    if (cmdLineArgs->dryRunPresent) {
      if (cmdLineArgs->numFixedArgs > 0) {
        char *fixedArgString = create_string_from_array(
            cmdLineArgs->fixedArgs, cmdLineArgs->numFixedArgs);
        printf("%i: %s %s %s", count, cmdLineArgs->command, fixedArgString,
               processedLine);
        count += 1;
        free(fixedArgString);
      } else if (cmdLineArgs->commandPresent) {
        printf("%i: %s %s", count, cmdLineArgs->command, processedLine);
        count += 1;
      } else {
        printf("%i: %s", count, processedLine);
        count += 1;
      }
    }

    free(processedLine);
    fflush(stdout);
  }
}

// Executes the appropriate dry-run printing function
// Inputs: cmdLineArgs - pointer to CLArgs struct
void execute_dry_run(const struct CLArgs *cmdLineArgs) {
  if (cmdLineArgs->perTaskPresent) {
    per_task_dry_run(cmdLineArgs);
  } else if (cmdLineArgs->argsFilePresent) {
    file_dry_run(cmdLineArgs);
  } else {
    stdin_dry_run(cmdLineArgs);
  }
}

// Executes a single child process for a pipe
// Inputs: pArgs - pointer to PArgs struct
//         i - child index
//         numChildren - total number of children
//         pipes - pipe file descriptor array
void exec_pipe_child(struct PArgs *pArgs, int i, int numChildren,
                     int pipes[][2]) {
  // if child isn't first, read last childs STDIN
  if (i > 0) {
    dup2(pipes[i - 1][0], STDIN_FILENO);
  }
  // if child isn't last, open stdout for next child to read
  if (i < numChildren - 1) {
    dup2(pipes[i][1], STDOUT_FILENO);
  }

  for (int j = 0; j < numChildren - 1; j++) {
    close(pipes[j][0]);
    close(pipes[j][1]);
  }

  if (!pArgs->args[i] || !pArgs->args[i][0]) {
    fprintf(stderr, "uqparallel: unable to execute empty command\n");
    exit(EMPTY_COMMAND_EXIT_NUM);
  }

  execvp(pArgs->args[i][0], pArgs->args[i]);
  fprintf(stderr, "uqparallel: cannot execute \"%s\"\n", pArgs->args[i][0]);
  raise(SIGUSR1);
  exit(SIGNAL_EXIT_NUM);
}

// Waits for and processes a child process termination
// Inputs: activeChildren - pointer to active child count
//         lastExitStatus - pointer to last exit status
void reap_child(int *activeChildren, int *lastExitStatus) {
  int status;
  wait(&status);
  (*activeChildren)--;
  if (WIFEXITED(status)) {
    *lastExitStatus = WEXITSTATUS(status);
  } else {
    *lastExitStatus = SIGNAL_EXIT_NUM;
  }
}

// Executes children in parallel using pipes
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
// Returns: exit code from last child
int make_pipe_babies(const struct CLArgs *cmdLineArgs, struct PArgs *pArgs) {
  int numChildren = pArgs->numArgs;
  int maxChildren = cmdLineArgs->jobLimit;
  int activeChildren = 0;
  int lastExitStatus = 0;

  // pipe() opens read and write ends of pipes
  int pipes[numChildren - 1][2];
  for (int i = 0; i < numChildren - 1; i++) {
    if (pipe(pipes[i]) == -1) {
      perror("pipe");
      exit(1);
    }
  }
  for (int i = 0; i < numChildren; i++) {
    while (activeChildren >= maxChildren) {
      int status;
      wait(&status);
      activeChildren--;
      if (WIFEXITED(status)) {
        lastExitStatus = WEXITSTATUS(status);
      } else {
        lastExitStatus = SIGNAL_EXIT_NUM;
      }
    }
    pid_t pid = fork();
    if (pid == 0) {
      exec_pipe_child(pArgs, i, numChildren, pipes);
    } else if (pid > 0) {
      activeChildren++;
    } else {
      perror("fork");
      return 1;
    }
  }
  // Close unused pipe fds in parent
  for (int i = 0; i < numChildren - 1; i++) {
    close(pipes[i][0]);
    close(pipes[i][1]);
  }
  // Reap remaining children
  while (activeChildren-- > 0) {
    reap_child(&activeChildren, &lastExitStatus);
  }

  return lastExitStatus;
}

// Executes a single child without pipes
// Inputs: pArgs - pointer to PArgs struct
//         i - index of command to execute
void exec_child(const struct PArgs *pArgs, int i) {
  if (!pArgs->args[i] || !pArgs->args[i][0]) {
    fprintf(stderr, "uqparallel: unable to execute empty command\n");
    exit(EMPTY_COMMAND_EXIT_NUM);
  }

  // Redirect stdout if needed
  if (pArgs->stdoutFiles && pArgs->stdoutFiles[i]) {
    int fd = open(pArgs->stdoutFiles[i], O_WRONLY | O_CREAT | O_TRUNC,
                  READ_WRITE_PERMISSIONS);
    if (fd < 0) {
      fprintf(stderr, "uqparallel: cannot write to \"%s\"\n",
              pArgs->stdoutFiles[i]);
      raise(SIGUSR1);
      exit(SIGNAL_EXIT_NUM);
    }
    dup2(fd, STDOUT_FILENO);
    close(fd);
  }

  // Redirect stderr if needed
  if (pArgs->stderrFiles && pArgs->stderrFiles[i]) {
    int fd = open(pArgs->stderrFiles[i], O_WRONLY | O_CREAT | O_TRUNC,
                  READ_WRITE_PERMISSIONS);
    if (fd < 0) {
      fprintf(stderr, "uqparallel: cannot write to \"%s\"\n",
              pArgs->stderrFiles[i]);
      raise(SIGUSR1);
      exit(SIGNAL_EXIT_NUM);
    }
    dup2(fd, STDERR_FILENO);
    close(fd);
  }

  execvp(pArgs->args[i][0], pArgs->args[i]);
  fprintf(stderr, "uqparallel: cannot execute \"%s\"\n", pArgs->args[i][0]);
  raise(SIGUSR1);
  exit(SIGNAL_EXIT_NUM);
}

// Executes children in parallel using fork/exec without piping
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
// Returns: exit code from last child
int make_babies(const struct CLArgs *cmdLineArgs, struct PArgs *pArgs) {
  int maxChildren = cmdLineArgs->jobLimit;
  int numChildren = pArgs->numArgs;
  int activeChildren = 0;
  int lastExitStatus = 0;

  for (int i = 0; i < numChildren; i++) {
    while (activeChildren >= maxChildren) {
      reap_child(&activeChildren, &lastExitStatus);
    }

    pid_t pid = fork();
    if (pid == 0) {
      exec_child(pArgs, i);
    } else if (pid > 0) {
      activeChildren++;
    } else {
      perror("fork");
      return 1;
    }
  }

  // Wait for all children to finish to reap them
  while (activeChildren-- > 0) {
    reap_child(&activeChildren, &lastExitStatus);
  }

  return lastExitStatus;
}

// Tokenizes and prepares a line of stdin input for execution, then sends to
// make_babies() Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
//         line - single line of stdin input
void process_stdin_line(const struct CLArgs *cmdLineArgs, struct PArgs *pArgs,
                        char *line) {
  line[strcspn(line, "\n")] = '\0';
  char *processedLine = modify_string(line);
  int numTokens = 0;
  char **tokens = split_space_not_quote(processedLine, &numTokens);
  int i = 0, writePointer = pArgs->stdinArgsPosition;

  pArgs->numElements[i] += numTokens;
  pArgs->args[i] = (char **)realloc(
      (void *)pArgs->args[i],
      (pArgs->numElements[i] + NULL_TERMINATOR) * sizeof(char *));

  for (int j = 0; j < numTokens; j++) {
    // if stdoutFile or stderrFile are found, remove a memory space from
    // pArgs->args remember the file and update a boolean if --pipe is not
    // present
    if (tokens[j][0] == stdoutFile) {
      pArgs->numElements[i]--;
      pArgs->args[i] = (char **)realloc((void *)pArgs->args[i],
                                        pArgs->numElements[i] * sizeof(char *));
      // if pipe not present then populate it with file to write to
      if (!cmdLineArgs->pipePresent) {
        free(pArgs->stdoutFiles[i]);
        pArgs->stdoutFiles[i] = strdup(tokens[j] + STDOUT_FILE_HEADER_LENGTH);
      }
    } else if (strncmp(tokens[j], stderrFile, 2) == 0) {
      pArgs->numElements[i]--;
      pArgs->args[i] = (char **)realloc((void *)pArgs->args[i],
                                        pArgs->numElements[i] * sizeof(char *));
      if (!cmdLineArgs->pipePresent) {
        free(pArgs->stderrFiles[i]);
        pArgs->stderrFiles[i] = strdup(tokens[j] + STDERR_FILE_HEADER_LENGTH);
      }
    } else {
      free(pArgs->args[i][writePointer + 1]);
      pArgs->args[i][writePointer++] = strdup(tokens[j]);
    }
  }
  // was getting double NULL values at the end for some reason, this fixed it
  pArgs->numElements[i] = writePointer + 1;
  pArgs->args[i] = (char **)realloc((void *)pArgs->args[i],
                                    pArgs->numElements[i] * sizeof(char *));
  pArgs->args[i][writePointer] = NULL;

  make_babies(cmdLineArgs, pArgs);
  free(processedLine);
  free((void *)tokens);
}

// Reads stdin line-by-line and sends them to process_stdin_line()
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
void make_babies_stdin_helper(const struct CLArgs *cmdLineArgs,
                              struct PArgs *pArgs) {
  char line[LINE_BUFFER];
  while (fgets(line, sizeof(line), stdin)) {
    process_stdin_line(cmdLineArgs, pArgs, line);
    fflush(stdout);
  }
}

// Handles execution logic, either dry-run or spawning children
// Inputs: cmdLineArgs - pointer to CLArgs struct
//         pArgs - pointer to PArgs struct
// Returns: exit code
int execute_commands(const struct CLArgs *cmdLineArgs, struct PArgs *pArgs) {
  if (cmdLineArgs->dryRunPresent) {
    execute_dry_run(cmdLineArgs);
    return 0;
  }

  if (cmdLineArgs->perTaskPresent || cmdLineArgs->argsFilePresent) {
    if (cmdLineArgs->pipePresent) {
      return make_pipe_babies(cmdLineArgs, pArgs);
    }
    return make_babies(cmdLineArgs, pArgs);
  }

  make_babies_stdin_helper(cmdLineArgs, pArgs);
  return 0;
}

// Validates --pipe usage based on presence of argsFile or :::
// Inputs: argc - argument count
//         argv - array of command-line arguments
// Returns: true is the command is valid
bool valid_pipe_command(int argc, char *argv[]) {
  bool pipePresent = false;
  bool argsFilePresent = false;
  bool perTaskPresent = false;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], pipeOption) == 0) {
      pipePresent = true;
    } else if (strcmp(argv[i], argsFile) == 0) {
      argsFilePresent = true;
    } else if (strncmp(argv[i], perTask, PER_TASK_LENGTH) == 0) {
      perTaskPresent = true;
    }
  }
  if (pipePresent == false) {
    return true;
  }

  if (pipePresent && (argsFilePresent || perTaskPresent)) {
    return true;
  }

  return false;
}

// Validates that strings aren't empty for commands and certain arguments
// Inputs: argc - argument count
//         argv - array of arguments
// Returns: false if an empty string is used as a command or an argument to
// jobLimit or argsFile
bool empty_string_validation(int argc, char *argv[]) {
  bool commandSeen = false;
  bool perTaskSeen = false;

  if (argc == 1 && argv[0][0] == '\0') {
    return false;
  }

  for (int i = 0; i < argc; i++) {
    // argsFile and jobLimit cant have empty string after it.
    if ((strcmp(argv[i], argsFile) == 0) || (strcmp(argv[i], jobLimit) == 0)) {
      // If a command or perTask has been seen, it will be treated as an
      // argument and can be followed by an empty string.
      if ((commandSeen == false) && (perTaskSeen == false)) {
        if ((i == argc - 1) || (strcmp(argv[i + 1], "") == 0)) {
          return false;
        }
      }
    }
    // anything starting with . - or / can't have an empty string before it
    // this makes sure a command isn't an empty string
    else if ((argv[i][0] == '.' || argv[i][0] == '-' || argv[i][0] == '/') &&
             (strncmp(argv[i], "--", OPTION_HEADER_LENGTH) != 0)) {
      if ((i > 0) && (strcmp(argv[i - 1], "") == 0)) {
        return false;
      }
    } else if (strcmp(argv[i], perTask) == 0) {
      perTaskSeen = true;
    } else if (strncmp(argv[i], "--", OPTION_HEADER_LENGTH) != 0) {
      commandSeen = true;
    }
  }
  return true;
}

// Checks jobLimit is in valid range
// Inputs: argc - argument count
//         argv - array of arguments
// Returns: true if jobLimit is not present or if jobLimit is followed by an
// integer between 1 and 20
bool job_limit_range_check(int argc, char *argv[]) {
  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], jobLimit) == 0) {
      int jobLimitValue = atoi(argv[i + 1]);
      if ((jobLimitValue < JOB_LIMIT_MIN) || (jobLimitValue > JOB_LIMIT_MAX)) {
        return false;
      }
    }
  }
  return true;
}

// Returns true if both --argsfile and ::: are present
// Inputs: argc - argument count
//         argv - array of arguments
// Returns: true is both argsFile and perTask are present in *argv[]
bool argsfile_and_per_task_present(int argc, char *argv[]) {
  bool argsFilePresent = false;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], argsFile) == 0) {
      argsFilePresent = true;
    } else if (strcmp(argv[i], perTask) == 0) {
      if (argsFilePresent) {
        return true;
      }
      return false;
    }
  }

  return false;
}

// Detects invalid options in the command-line arguments
// Inputs: argc - argument count
//         argv - array of arguments
// Returns: true if any invalid options are found in the command line
bool invalid_options(int argc, char *argv[]) {
  for (int i = 0; i < argc; i++) {
    // if per task is seen then invalid options can be present
    if (strcmp(argv[i], perTask) == 0) {
      return false;
    }
    // iterate past joblimit and argsfile arguments if found
    if (strcmp(argv[i], jobLimit) == 0 && i != argc - 1) {
      i++;
    }
    if (strcmp(argv[i], argsFile) == 0 && i != argc - 1) {
      i++;
    }
    // commands can have invalid options after them
    if (strncmp(argv[i], "--", OPTION_HEADER_LENGTH) != 0) {
      return false;
    }
    // we already skip past joblimit and argsfile before this point
    // so if this command is not pipe, exit on error, or dry run
    // then it is invalid
    if (strncmp(argv[i], "--", OPTION_HEADER_LENGTH) == 0) {
      if (strcmp(argv[i], pipeOption) != 0 &&
          strcmp(argv[i], exitOnError) != 0 && strcmp(argv[i], dryRun) != 0) {
        return true;
      }
    }
  }
  return false;
}

// Top-level validation of command-line argument rules
// Inputs: argc - argument count
//         argv - array of arguments
// Returns: true if the command line arguments are valid
bool valid_command_line_args(int argc, char *argv[]) {
  // if no command line args then all good
  if (argc == 0) {
    return true;
  }

  // check that if pipe is present, argsFile or ::: is also present
  if (valid_pipe_command(argc, argv) == false) {
    return false;
  }

  // check if ::: follows ---argsfile option
  if (argsfile_and_per_task_present(argc, argv) == true) {
    return false;
  }

  // check that commands or certain arguments aren't empty strings
  if (empty_string_validation(argc, argv) == false) {
    return false;
  }

  // check that the integer given for job limit is in range
  if (job_limit_range_check(argc, argv) == false) {
    return false;
  }

  // check for any invalid options
  if (invalid_options(argc, argv)) {
    return false;
  }

  return true;
}

// Main entry point. Parses args, runs execution (makes babies), and frees
// memory. Inputs: argc - number of command-line arguments
//         argv - command-line arguments array
// Returns: program exit code
int main(int argc, char *argv[]) {
  // validate command line arguments, don't send argv[0]
  if (argc > 1) {
    if (valid_command_line_args(argc - 1, argv + 1) == false) {
      fprintf(stderr, "%s", usageErrorMessage);
      exit(USAGE_ERROR_EXIT_NUM);
    }
  }

  // make struct that contains command line args
  struct CLArgs *cmdLineArgs = NULL;
  struct PArgs *pArgs = NULL;

  cmdLineArgs = command_line_struct_creator(argc - 1, argv + 1);
  pArgs = process_struct_creator(cmdLineArgs);

  int exitCode = execute_commands(cmdLineArgs, pArgs);

  // Free CLArgs struct at end of program
  free_command_line_struct(cmdLineArgs);
  free_process_struct(pArgs);

  return exitCode;
}
