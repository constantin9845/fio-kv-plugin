
#include "include/var_key.h"

double KEY_DISTRIBUTION_STATUS = 0;
double SMALL_KEY_COUNTER = 0;
double LARGE_KEY_COUNTER = 0;

_Atomic double IO_COUNTER = 0;
_Atomic double IO_COUNTER_READ = 0;
_Atomic double IO_COUNTER_WRITE = 0;



// Keys
int key_1 = 0;
int key_2 = 0;
int key_3 = 0;
int key_4 = 0;
int key_5 = 0;
int key_6 = 0;

int target_key_ratio_1 = 0;
int target_key_ratio_2 = 0;
int target_key_ratio_3 = 0;
int target_key_ratio_4 = 0;
int target_key_ratio_5 = 0;
int target_key_ratio_6 = 0;

_Atomic double KEY_COUNTER_READ_1 = 1;
_Atomic double KEY_COUNTER_READ_2 = 1;
_Atomic double KEY_COUNTER_READ_3 = 1;
_Atomic double KEY_COUNTER_READ_4 = 1;
_Atomic double KEY_COUNTER_READ_5 = 1;
_Atomic double KEY_COUNTER_READ_6 = 1;

_Atomic double KEY_COUNTER_WRITE_1 = 1;
_Atomic double KEY_COUNTER_WRITE_2 = 1;
_Atomic double KEY_COUNTER_WRITE_3 = 1;
_Atomic double KEY_COUNTER_WRITE_4 = 1;
_Atomic double KEY_COUNTER_WRITE_5 = 1;
_Atomic double KEY_COUNTER_WRITE_6 = 1;


// values
int value_1 = 0;
int value_2 = 0;
int value_3 = 0;
int value_4 = 0;
int value_5 = 0;

int value_target_1 = 0;
int value_target_2 = 0;
int value_target_3 = 0;
int value_target_4 = 0;
int value_target_5 = 0;


_Atomic double VALUE_COUNTER_READ_1 = 1;
_Atomic double VALUE_COUNTER_READ_2 = 1;
_Atomic double VALUE_COUNTER_READ_3 = 1;
_Atomic double VALUE_COUNTER_READ_4 = 1;
_Atomic double VALUE_COUNTER_READ_5 = 1;

_Atomic double VALUE_COUNTER_WRITE_1 = 1;
_Atomic double VALUE_COUNTER_WRITE_2 = 1;
_Atomic double VALUE_COUNTER_WRITE_3 = 1;
_Atomic double VALUE_COUNTER_WRITE_4 = 1;
_Atomic double VALUE_COUNTER_WRITE_5 = 1;