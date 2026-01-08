
#include "include/var_key.h"

double KEY_DISTRIBUTION_STATUS = 0;
double SMALL_KEY_COUNTER = 0;
double LARGE_KEY_COUNTER = 0;

double IO_COUNTER = 0;

int target_r512B = 100;
int target_r1KB = 0;
int target_r2KB = 0;
int target_r3KB = 0;
int target_r4KB = 0;

double R512B_COUNTER = 0;
double R1KB_COUNTER = 0;
double R2KB_COUNTER = 0;
double R3KB_COUNTER = 0;
double R4KB_COUNTER = 0;

int LAST_IO_TYPE = 0;