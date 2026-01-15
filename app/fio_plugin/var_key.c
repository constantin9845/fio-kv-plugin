
#include "include/var_key.h"

double KEY_DISTRIBUTION_STATUS = 0;
double SMALL_KEY_COUNTER = 0;
double LARGE_KEY_COUNTER = 0;

_Atomic double IO_COUNTER = 0;
_Atomic double IO_COUNTER_READ = 0;
_Atomic double IO_COUNTER_WRITE = 0;

int target_64 = 0;
int target_128 = 0;
int target_256 = 0;
int target_512 = 0;
int target_1024 = 0;


_Atomic double COUNTER_READ_64 = 1;
_Atomic double COUNTER_READ_128 = 1;
_Atomic double COUNTER_READ_256 = 1;
_Atomic double COUNTER_READ_512 = 1;
_Atomic double COUNTER_READ_1024 = 1;

_Atomic double COUNTER_WRITE_64 = 1;
_Atomic double COUNTER_WRITE_128 = 1;
_Atomic double COUNTER_WRITE_256 = 1;
_Atomic double COUNTER_WRITE_512 = 1;
_Atomic double COUNTER_WRITE_1024 = 1;