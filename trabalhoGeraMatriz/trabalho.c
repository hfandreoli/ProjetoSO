#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <pthread.h>
#include <string.h>

#define MAX_INPUT 20
#define MAX_NAME_LENGTH 20
#define MAX_FILE_SIZE 20000
#define MAX_THREADS 16

typedef struct Buffer {
    int *numbers;
    int top;
} Buffer;

typedef struct ThreadData {
    int number;
    int low;
    int high;
} ThreadData;

// Variáveis globais
Buffer **buffers;                   //buffers para os números dos arquivos
char **inputFilesNames;             //nomes dos arquivos
int biggestFileSize;                //número máximo de entradas entre os arquivos
int n_threads;                      //número de threads
int n_inputFiles;                   //número de arquivos
int **outputMatrix;                 //matriz de saída
pthread_t threadIDs[MAX_THREADS];   //Vetor que armazena o id das threads
sem_t mutex;                        //semáforo para exclusão mútua
int *vector;                        //array com todos os números
int ready[MAX_THREADS];             //array para status da ordenação de cada thread
ThreadData *threadDataList;         //vetor com argumentos para as threads
int totalSize;


// função para a leitura de um arquivo em um buffer
Buffer *readFile(char *filename) {
    FILE *file = fopen(filename, "r");
    int number;

    Buffer *buffer = (Buffer *) malloc(sizeof(Buffer));
    buffer->numbers = (int *) malloc(sizeof(int) * MAX_FILE_SIZE);
    buffer->top = 0;

    if (file != NULL) {
        do {
            fscanf(file, "%d", &number);
            buffer->numbers[buffer->top++] = number;
            number = fgetc(file);
        } while (number != EOF);
    }

    fclose(file);
    return buffer;
}

// lógica de thread para leitura dos arquivos
void *threadReadFile(void *arg) {
    int thrdId = *(int *) arg;
    int currFile = thrdId;
    Buffer *buffer;

    while (currFile < n_inputFiles) {
        buffer = readFile(inputFilesNames[currFile]);
        sem_wait(&mutex);
        if (buffer->top > biggestFileSize) {
            biggestFileSize = buffer->top;
        }
        sem_post(&mutex);

        buffers[currFile] = buffer;
        currFile += n_threads;
    }

    return NULL;
}

// lógica de merge para merge sort
void merge(int low, int mid, int high) {
    int sizeLeft = mid - low + 1;
    int sizeRight = high - mid;

    int *leftHalf = malloc(sizeLeft * sizeof(int));
    int *rightHalf = malloc(sizeRight * sizeof(int));

    int i;
    int j;

    for (i = 0; i < sizeLeft; i++)
        leftHalf[i] = vector[i + low];

    for (i = 0; i < sizeRight; i++)
        rightHalf[i] = vector[i + mid + 1];

    int k = low;
    i = j = 0;

    while (i < sizeLeft && j < sizeRight) {
        if (leftHalf[i] <= rightHalf[j]) {
            vector[k++] = leftHalf[i++];
        } else {
            vector[k++] = rightHalf[j++];
        }
    }

    while (i < sizeLeft) {
        vector[k++] = leftHalf[i++];
    }

    while (j < sizeRight) {
        vector[k++] = rightHalf[j++];
    }

    free(leftHalf);
    free(rightHalf);
}

void waitAllThreads() {
    //segura a execução até que todas estejam prontas
    int allReady;
    do {
        allReady = 1;
        for (int i = 0; i < n_threads; i++) {
            if (ready[i] == 0) {
                allReady = 0;
            }
        }
    } while (allReady == 0);
}

void *threadInsertionSort(void *arg) {
    ThreadData *threadData = arg;
    int low = threadData->low;
    int high = threadData->high;
    int size = high - low + 1;
    int j, key;

    // cada thread ordena um parte do vetor
    for (int i = 1; i < size; i++) {
        key = vector[i + low];
        j = i + low - 1;
        while (j >= low && vector[j] > key) {
            vector[j + 1] = vector[j];
            j = j - 1;
        }
        vector[j + 1] = key;
    }
    ready[threadData->number] = 1;

    // esperar
    waitAllThreads();

    // thread 0 realiza a ordenação final com merge
    if (threadData->number == 0) {
        ready[threadData->number] = 0;
        //juntar as partes ordenadas
        ThreadData *firstThreadData = &threadDataList[0];
        ThreadData *threadDataRecord;
        for (int i = 1; i < n_threads; i++) {
            threadDataRecord = &threadDataList[i];
            merge(firstThreadData->low, threadDataRecord->low - 1, threadDataRecord->high);
        }
        ready[threadData->number] = 1;
    }

    // esperar
    waitAllThreads();

    // montagem da matriz
    // a montagem da tratiz final é dividida entre as threads
    int line = threadData->number;

    while (line < n_inputFiles) {
        low = line * biggestFileSize;
        int n = low;

        for (int i = 0; i < biggestFileSize; i++) {
            if (n < totalSize) {
                outputMatrix[line][i] = vector[n++];
            } else {
                outputMatrix[line][i] = 0;
            }
        }

        line += n_threads;
    }

    return NULL;
}

int main(int argc, char **argv) {
    char *outputFileName;

    // vetor de números para que sues endereços sejam passados como argumento para as threads
    int numbers[MAX_THREADS];
    for (int n = 0; n < MAX_THREADS; n++) {
        numbers[n] = n;
    }

    // leitura dos argumentos
    n_threads = atoi(argv[1]);

    inputFilesNames = (char **) malloc(MAX_INPUT * (sizeof(char *)));
    n_inputFiles = 0;
    do {
        inputFilesNames[n_inputFiles] = (char *) malloc(MAX_NAME_LENGTH * sizeof(char));
        strcpy(inputFilesNames[n_inputFiles], argv[n_inputFiles + 2]);
        n_inputFiles++;
    } while (strcmp(argv[n_inputFiles + 2], "-o") != 0);

    outputFileName = (char *) malloc(MAX_NAME_LENGTH * sizeof(char));
    strcpy(outputFileName, argv[n_inputFiles + 3]);

    // inicialização dos semáforos
    sem_init(&mutex, 0, 1);

    for (int i = 0; i < MAX_THREADS; i++) {
        ready[i] = 0;
    }

    // inicialização dos buffers
    buffers = (Buffer **) malloc(sizeof(Buffer *) * n_inputFiles);
    biggestFileSize = 0;

    //leitrura dos arquivos dividida entre as threads
    for (int i = 0; i < n_threads; i++) {
        pthread_create(&threadIDs[i], NULL, threadReadFile, &numbers[i]);
    }
    for (int j = n_threads - 1; j >= 0; j--) {
        pthread_join(threadIDs[j], NULL);
    }

    //inicialização e montagem do vetor total
    totalSize = 0;
    for (int i = 0; i < n_inputFiles; i++) {
        totalSize += buffers[i]->top;
    }
    vector = (int *) malloc(totalSize * sizeof(int));
    int n = 0;
    for (int i = 0; i < n_inputFiles; i++) {
        for (int j = 0; j < buffers[i]->top; j++) {
            vector[n++] = buffers[i]->numbers[j];
        }
    }

    //divisão das partes do vetor para as threads de ordenação
    threadDataList = (ThreadData *) malloc(sizeof(ThreadData) * n_threads);
    int length = totalSize / n_threads;

    for (int l = 0; l < n_threads; l++) {
        threadDataList[l].number = l;
        threadDataList[l].low = l * length;

        if (l == n_threads - 1) {
            threadDataList[l].high = totalSize - 1;
        } else {
            threadDataList[l].high = threadDataList[l].low + length - 1;
        }
    }

    // inicialização da matriz de saída
    outputMatrix = (int **) malloc(sizeof(int *) * n_inputFiles);
    for (int i = 0; i < n_inputFiles; i++) {
        outputMatrix[i] = (int *) malloc(sizeof(int) * biggestFileSize);
    }


    time_t t1 = time(NULL);
    //divisão do sort entre as threads
    for (int m = 0; m < n_threads; m++) {
        pthread_create(&threadIDs[m], NULL, threadInsertionSort, &threadDataList[m]);
    }
    for (int j = n_threads - 1; j >= 0; j--) {
        pthread_join(threadIDs[j], NULL);
    }
    time_t t2 = time(NULL);


    // gera arquivo de saída
    FILE *output = fopen(outputFileName, "w");
    for (int i = 0; i < n_inputFiles; i++) {
        for (int j = 0; j < biggestFileSize; j++) {
            fprintf(output, "%d\t\t", outputMatrix[i][j]);
        }
        if (i < n_inputFiles - 1) {
            fprintf(output, "\n");
        }
    }

    printf("Arquivo \"%s\" gerado!\n", outputFileName);
    printf("Total de entradas: %d\n", totalSize);
    printf("Threads: %d\n", n_threads);
    printf("Tempo: %.0lf segundos\n", (double) (t2 - t1));

    return 0;
}