#ifndef VAR_KEY_H
#define VAR_KEY_H

#include <stdlib.h>
#include <stdio.h>

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

static inline u_int32_t get_kv_value_size(){
	// 28KB to 2048KB
	// unit : 1 = 1KB

	// 512 bytes
	// 1KB
	// 2KB
	// 3KB
	// 4KB

	int min = 28;
	int max = 2048;

	return (u_int32_t)((rand() % (max - min + 1)) + min);
}

static inline u_int64_t splitmix64(u_int64_t *x){
	u_int64_t z = (*x += 0x9e3779b97f4a7c15ULL);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

#endif