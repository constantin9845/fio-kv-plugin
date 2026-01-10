
#include "include/var_key.h"

double KEY_DISTRIBUTION_STATUS = 0;
double SMALL_KEY_COUNTER = 0;
double LARGE_KEY_COUNTER = 0;

_Atomic double IO_COUNTER = 0;
_Atomic double IO_COUNTER_READ = 0;
_Atomic double IO_COUNTER_WRITE = 0;

int target_r512B = 100;
int target_r1KB = 0;
int target_r2KB = 0;
int target_r3KB = 0;
int target_r4KB = 0;


_Atomic double R512B_COUNTER_READ = 1;
_Atomic double R1KB_COUNTER_READ = 1;
_Atomic double R2KB_COUNTER_READ = 1;
_Atomic double R3KB_COUNTER_READ = 1;
_Atomic double R4KB_COUNTER_READ = 1;

_Atomic double R512B_COUNTER_WRITE = 1;
_Atomic double R1KB_COUNTER_WRITE = 1;
_Atomic double R2KB_COUNTER_WRITE = 1;
_Atomic double R3KB_COUNTER_WRITE = 1;
_Atomic double R4KB_COUNTER_WRITE = 1;