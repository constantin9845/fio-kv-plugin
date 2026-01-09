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

extern _Atomic double R512B_COUNTER_READ;
extern _Atomic double R1KB_COUNTER_READ;
extern _Atomic double R2KB_COUNTER_READ;
extern _Atomic double R3KB_COUNTER_READ;
extern _Atomic double R4KB_COUNTER_READ;

extern _Atomic double R512B_COUNTER_WRITE;
extern _Atomic double R1KB_COUNTER_WRITE;
extern _Atomic double R2KB_COUNTER_WRITE;
extern _Atomic double R3KB_COUNTER_WRITE;
extern _Atomic double R4KB_COUNTER_WRITE;

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

	if(prob < (u_int32_t)target_r512B){ 
		if(is_read){
			R512B_COUNTER_READ++;
		}
		else{
			R512B_COUNTER_WRITE++;
		}
		return (u_int32_t)512; 
	}

	if(prob < (u_int32_t)target_r512B + (u_int32_t)target_r1KB){ 
		if(is_read){
			R1KB_COUNTER_READ++;
		}
		else{
			R1KB_COUNTER_WRITE++;
		}
		return (u_int32_t)1024; 
	}

	if(prob < (u_int32_t)target_r512B + (u_int32_t)target_r1KB + (u_int32_t)target_r2KB){ 
		if(is_read){
			R2KB_COUNTER_READ++;
		}
		else{
			R2KB_COUNTER_WRITE++;
		}
		return (u_int32_t)2048; 
	}

	if(prob < (u_int32_t)target_r512B + (u_int32_t)target_r1KB + (u_int32_t)target_r2KB + (u_int32_t)target_r3KB){ 
		if(is_read){
			R3KB_COUNTER_READ++;
		}
		else{
			R3KB_COUNTER_WRITE++;
		}
		return (u_int32_t)3072; 
	}

	if(is_read){
		R4KB_COUNTER_READ++;
	}
	else{
		R4KB_COUNTER_WRITE++;
	}
	return (u_int32_t)4096;
}

#endif