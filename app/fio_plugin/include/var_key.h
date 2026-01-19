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

// Keys
extern int target_key_4;
extern int target_key_8;
extern int target_key_16;
extern int target_key_32;
extern int target_key_64;
extern int target_key_128;

extern _Atomic double KEY_COUNTER_READ_4;
extern _Atomic double KEY_COUNTER_READ_8;
extern _Atomic double KEY_COUNTER_READ_16;
extern _Atomic double KEY_COUNTER_READ_32;
extern _Atomic double KEY_COUNTER_READ_64;
extern _Atomic double KEY_COUNTER_READ_128;

extern _Atomic double KEY_COUNTER_WRITE_4;
extern _Atomic double KEY_COUNTER_WRITE_8;
extern _Atomic double KEY_COUNTER_WRITE_16;
extern _Atomic double KEY_COUNTER_WRITE_32;
extern _Atomic double KEY_COUNTER_WRITE_64;
extern _Atomic double KEY_COUNTER_WRITE_128;


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

static inline u_int32_t get_kv_key_size(u_int64_t seed, bool is_read){
	// generates stable value in range 0-99
	u_int64_t temp_seed = seed;
	u_int32_t prob = splitmix64(&temp_seed) % 100;

	if(prob < (u_int32_t)target_key_4){ 
		if(is_read){
			KEY_COUNTER_READ_4++;
		}
		else{
			KEY_COUNTER_WRITE_4++;
		}
		return (u_int32_t)4; 
	}

	if(prob < (u_int32_t)target_key_4 + (u_int32_t)target_key_8){ 
		if(is_read){
			KEY_COUNTER_READ_8++;
		}
		else{
			KEY_COUNTER_WRITE_8++;
		}
		return (u_int32_t)8; 
	}

	if(prob < (u_int32_t)target_key_4 + (u_int32_t)target_key_8 + (u_int32_t)target_key_16){ 
		if(is_read){
			KEY_COUNTER_READ_16++;
		}
		else{
			KEY_COUNTER_WRITE_16++;
		}
		return (u_int32_t)16; 
	}

	if(prob < (u_int32_t)target_key_4 + (u_int32_t)target_key_8 + (u_int32_t)target_key_16 + (u_int32_t)target_key_32){ 
		if(is_read){
			KEY_COUNTER_READ_32++;
		}
		else{
			KEY_COUNTER_WRITE_32++;
		}
		return (u_int32_t)32; 
	}

	if(prob < (u_int32_t)target_key_4 + (u_int32_t)target_key_8 + (u_int32_t)target_key_16 + (u_int32_t)target_key_32 + (u_int32_t)target_key_64){ 
		if(is_read){
			KEY_COUNTER_READ_64++;
		}
		else{
			KEY_COUNTER_WRITE_64++;
		}
		return (u_int32_t)64; 
	}

	if(is_read){
		KEY_COUNTER_READ_128++;
	}
	else{
		KEY_COUNTER_WRITE_128++;
	}
	return (u_int32_t)128;
}

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