#ifndef VAR_KEY_H
#define VAR_KEY_H

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdatomic.h>

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
extern int target_r512B;
extern int target_r1KB;
extern int target_r2KB;
extern int target_r3KB;
extern int target_r4KB;

extern _Atomic double R512B_COUNTER;
extern _Atomic double R1KB_COUNTER;
extern _Atomic double R2KB_COUNTER;
extern _Atomic double R3KB_COUNTER;
extern _Atomic double R4KB_COUNTER;

extern _Atomic double IO_COUNTER;

// 0 none
// 1 512 | 2 1kb | 3 2kb | 4 3kb | 5 4kb
extern int LAST_IO_TYPE;

static inline bool ratio_satisfied(double counter, int target){

	if(IO_COUNTER == 0){
		return false;
	}

	double status = counter/IO_COUNTER;

	return status >= target;
}

// start with largest values first and move smaller as ration satisfied
static inline u_int32_t get_kv_value_size(){
	// Type 1 : 512 bytes
	// Type 2 : 1KB
	// Type 3 : 2KB
	// Type 4 : 3KB
	// Type 5 : 4KB

	if(!ratio_satisfied(R512B_COUNTER, target_r512B)){
		LAST_IO_TYPE = 1;
		return (u_int32_t)512;
	}
	else if(!ratio_satisfied(R1KB_COUNTER, target_r1KB)){
		LAST_IO_TYPE = 2;
		return (u_int32_t)1024;
	}
	else if(!ratio_satisfied(R2KB_COUNTER, target_r2KB)){
		LAST_IO_TYPE = 3;
		return (u_int32_t)2048;
	}
	else if(!ratio_satisfied(R3KB_COUNTER, target_r3KB)){
		LAST_IO_TYPE = 4;
		return (u_int32_t)3072;
	}
	else if(!ratio_satisfied(R4KB_COUNTER, target_r4KB)){
		LAST_IO_TYPE = 5;
		return (u_int32_t)4096;
	}
	else{
		LAST_IO_TYPE = 1;
		return (u_int32_t)512;
	}
}

static inline u_int64_t splitmix64(u_int64_t *x){
	u_int64_t z = (*x += 0x9e3779b97f4a7c15ULL);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

#endif