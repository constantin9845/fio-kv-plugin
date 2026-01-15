#ifndef VAR_KEY_H
#define VAR_KEY_H

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <string.h>

/* VARIABLE KEY SIZE FUNCTIONS AND VARIABLES */

extern double KEY_DISTRIBUTION_STATUS;
extern double SMALL_KEY_COUNTER;
extern double LARGE_KEY_COUNTER;

// 4 to 255 bytes
static inline int get_kv_key_size(double kd){

	// small keys = 4 ~ 32 bytes [4 8 16 32]
	// large keys = 33 ~ 255 bytes [64 128]

	int k;

	if(KEY_DISTRIBUTION_STATUS < kd){
		LARGE_KEY_COUNTER++;
		int min = 1; 
		int max = 2;
		k = (rand() % (max - min + 1)) + min;
		switch(k){
			case 1:
				k = 64;
				break;
			case 2:
				k = 128;
				break;
		}
	}
	else{
		SMALL_KEY_COUNTER++;
		int min = 1; 
		int max = 4;
		k = (rand() % (max - min + 1)) + min;
		switch(k){
			case 1:
				k = 4;
				break;
			case 2:
				k = 8;
				break;
			case 3:
				k = 16;
				break;
			case 4:
				k = 32;
				break;
		}
	}

	if(SMALL_KEY_COUNTER == 0)
		KEY_DISTRIBUTION_STATUS = 1;
	else
		KEY_DISTRIBUTION_STATUS = LARGE_KEY_COUNTER / SMALL_KEY_COUNTER;

	return k;
}

// must sum to 100
extern int target_64;
extern int target_128;
extern int target_256;
extern int target_512;
extern int target_1024;

extern _Atomic double COUNTER_READ_64;
extern _Atomic double COUNTER_READ_128;
extern _Atomic double COUNTER_READ_256;
extern _Atomic double COUNTER_READ_512;
extern _Atomic double COUNTER_READ_1024;

extern _Atomic double COUNTER_WRITE_64;
extern _Atomic double COUNTER_WRITE_128;
extern _Atomic double COUNTER_WRITE_256;
extern _Atomic double COUNTER_WRITE_512;
extern _Atomic double COUNTER_WRITE_1024;

extern _Atomic double IO_COUNTER;
extern _Atomic double IO_COUNTER_READ;
extern _Atomic double IO_COUNTER_WRITE;

static inline u_int64_t splitmix64(u_int64_t *x){
	u_int64_t z = (*x += 0x9e3779b97f4a7c15ULL);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

// start with largest values first and move smaller as ration satisfied
static inline u_int32_t get_kv_value_size(u_int64_t seed, bool is_read){

	// generates stable value in range 0-99
	u_int64_t temp_seed = seed;
	u_int32_t prob = splitmix64(&temp_seed) % 100;

	if(prob < (u_int32_t)target_64){ 
		if(is_read){
			COUNTER_READ_64++;
		}
		else{
			COUNTER_WRITE_64++;
		}
		return (u_int32_t)64; 
	}

	
	if(prob < (u_int32_t)target_64 + (u_int32_t)target_128){ 
		if(is_read){
			COUNTER_READ_128++;
		}
		else{
			COUNTER_WRITE_128++;
		}
		return (u_int32_t)128; 
	}

	if(prob < (u_int32_t)target_64 + (u_int32_t)target_128 + (u_int32_t)target_256){ 
		if(is_read){
			COUNTER_READ_256++;
		}
		else{
			COUNTER_WRITE_256++;
		}
		return (u_int32_t)256; 
	}

	if(prob < (u_int32_t)target_64 + (u_int32_t)target_128 + (u_int32_t)target_256 + (u_int32_t)target_512){ 
		if(is_read){
			COUNTER_READ_512++;
		}
		else{
			COUNTER_WRITE_512++;
		}
		return (u_int32_t)512; 
	}

	if(is_read){
		COUNTER_READ_1024++;
	}
	else{
		COUNTER_WRITE_1024++;
	}
	return (u_int32_t)1024;
	
}

#endif