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
extern int key_1;
extern int key_2;
extern int key_3;
extern int key_4;
extern int key_5;
extern int key_6;

extern int target_key_ratio_1;
extern int target_key_ratio_2;
extern int target_key_ratio_3;
extern int target_key_ratio_4;
extern int target_key_ratio_5;
extern int target_key_ratio_6;

extern _Atomic double KEY_COUNTER_READ_1;
extern _Atomic double KEY_COUNTER_READ_2;
extern _Atomic double KEY_COUNTER_READ_3;
extern _Atomic double KEY_COUNTER_READ_4;
extern _Atomic double KEY_COUNTER_READ_5;
extern _Atomic double KEY_COUNTER_READ_6;

extern _Atomic double KEY_COUNTER_WRITE_1;
extern _Atomic double KEY_COUNTER_WRITE_2;
extern _Atomic double KEY_COUNTER_WRITE_3;
extern _Atomic double KEY_COUNTER_WRITE_4;
extern _Atomic double KEY_COUNTER_WRITE_5;
extern _Atomic double KEY_COUNTER_WRITE_6;


// Values
extern int value_1;
extern int value_2;
extern int value_3;
extern int value_4;
extern int value_5;
extern int value_6;

extern int value_target_1;
extern int value_target_2;
extern int value_target_3;
extern int value_target_4;
extern int value_target_5;

extern _Atomic double VALUE_COUNTER_READ_1;
extern _Atomic double VALUE_COUNTER_READ_2;
extern _Atomic double VALUE_COUNTER_READ_3;
extern _Atomic double VALUE_COUNTER_READ_4;
extern _Atomic double VALUE_COUNTER_READ_5;

extern _Atomic double VALUE_COUNTER_WRITE_1;
extern _Atomic double VALUE_COUNTER_WRITE_2;
extern _Atomic double VALUE_COUNTER_WRITE_3;
extern _Atomic double VALUE_COUNTER_WRITE_4;
extern _Atomic double VALUE_COUNTER_WRITE_5;

extern _Atomic double IO_COUNTER;
extern _Atomic double IO_COUNTER_READ;
extern _Atomic double IO_COUNTER_WRITE;

static inline void init_keys(int key_size[6], int key_ratio[6]){
	key1 = key_size[0]; 
	key2 = key_size[1]; 
	key3 = key_size[2]; 
	key4 = key_size[3]; 
	key5 = key_size[4]; 
	key6 = key_size[5];

	target_key_ratio1 = key_ratio[0]; 
	target_key_ratio2 = key_ratio[1]; 
	target_key_ratio3 = key_ratio[2]; 
	target_key_ratio4 = key_ratio[3]; 
	target_key_ratio5 = key_ratio[4]; 
	target_key_ratio6 = key_ratio[5];  
}

static inline void init_values(int value_size[5], int value_ratio[5]){
 
	value1 = value_size[0]; 
	value2 = value_size[1]; 
	value3 = value_size[2]; 
	value4 = value_size[3]; 
	value5 = value_size[4]; 

	value_target1 = value_ratio[0]; 
	value_target2 = value_ratio[1]; 
	value_target3 = value_ratio[2]; 
	value_target4 = value_ratio[3]; 
	value_target5 = value_ratio[4]; 
}

static inline u_int64_t splitmix64(u_int64_t *x){
	u_int64_t z = (*x += 0x9e3779b97f4a7c15ULL);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

static inline u_int32_t get_kv_key_size(u_int64_t prob, bool is_read){

	if(
		prob < (u_int32_t)target_key_ratio_1
	){ 
		if(is_read){
			KEY_COUNTER_READ_1++;
		}
		else{
			KEY_COUNTER_WRITE_1++;
		}
		return (u_int32_t)key_1; 
	}

	if(
		prob < (u_int32_t)target_key_ratio_1 
			+ (u_int32_t)target_key_ratio_2
		){ 
		if(is_read){
			KEY_COUNTER_READ_2++;
		}
		else{
			KEY_COUNTER_WRITE_2++;
		}
		return (u_int32_t)key_2; 
	}

	if(
		prob < (u_int32_t)target_key_ratio_1 
			+ (u_int32_t)target_key_ratio_2 
			+ (u_int32_t)target_key_ratio_3
		){ 
		if(is_read){
			KEY_COUNTER_READ_3++;
		}
		else{
			KEY_COUNTER_WRITE_3++;
		}
		return (u_int32_t)key_3; 
	}

	if(
		prob < (u_int32_t)target_key_ratio_1 
			+ (u_int32_t)target_key_ratio_2 
			+ (u_int32_t)target_key_ratio_3 
			+ (u_int32_t)target_key_ratio_4
		){ 
		if(is_read){
			KEY_COUNTER_READ_4++;
		}
		else{
			KEY_COUNTER_WRITE_4++;
		}
		return (u_int32_t)key_4; 
	}

	if(
		prob < (u_int32_t)target_key_ratio_1 
			+ (u_int32_t)target_key_ratio_2 
			+ (u_int32_t)target_key_ratio_3 
			+ (u_int32_t)target_key_ratio_4
			+ (u_int32_t)target_key_ratio_5
	){ 
		if(is_read){
			KEY_COUNTER_READ_5++;
		}
		else{
			KEY_COUNTER_WRITE_5++;
		}
		return (u_int32_t)key_5; 
	}

	if(is_read){
		KEY_COUNTER_READ_6++;
	}
	else{
		KEY_COUNTER_WRITE_6++;
	}
	return (u_int32_t)key_6;
}

static inline u_int32_t get_kv_value_size(u_int64_t prob, bool is_read){

	if(
		prob < (u_int32_t)value_target_1
	){ 
		if(is_read){
			VALUE_COUNTER_READ_1++;
		}
		else{
			VALUE_COUNTER_WRITE_1++;
		}
		return (u_int32_t)value_1; 
	}

	if(
		prob < (u_int32_t)value_target_1
			+ (u_int32_t)value_target_2
	){ 
		if(is_read){
			VALUE_COUNTER_READ_2++;
		}
		else{
			VALUE_COUNTER_WRITE_2++;
		}
		return (u_int32_t)value_2; 
	}

	if(
		prob < (u_int32_t)value_target_1
			+ (u_int32_t)value_target_2
			+ (u_int32_t)value_target_3
	){ 
		if(is_read){
			VALUE_COUNTER_READ_3++;
		}
		else{
			VALUE_COUNTER_WRITE_3++;
		}
		return (u_int32_t)value_3; 
	}

	if(
		prob < (u_int32_t)value_target_1
			+ (u_int32_t)value_target_2
			+ (u_int32_t)value_target_3
			+ (u_int32_t)value_target_4
	){ 
		if(is_read){
			VALUE_COUNTER_READ_4++;
		}
		else{
			VALUE_COUNTER_WRITE_4++;
		}
		return (u_int32_t)value_4; 
	}

	if(is_read){
		VALUE_COUNTER_READ_5++;
	}
	else{
		VALUE_COUNTER_WRITE_5++;
	}
	return (u_int32_t)value_5;
	
}

static inline void reset(void){
	KEY_DISTRIBUTION_STATUS = 0;
	SMALL_KEY_COUNTER = 0;
	LARGE_KEY_COUNTER = 0;

	IO_COUNTER = 0;
	IO_COUNTER_READ = 0;
	IO_COUNTER_WRITE = 0;


	COUNTER_READ_64 = 1;
	COUNTER_READ_128 = 1;
	COUNTER_READ_256 = 1;
	COUNTER_READ_512 = 1;
	COUNTER_READ_1024 = 1;

	COUNTER_WRITE_64 = 1;
	COUNTER_WRITE_128 = 1;
	COUNTER_WRITE_256 = 1;
	COUNTER_WRITE_512 = 1;
	COUNTER_WRITE_1024 = 1;

	KEY_COUNTER_READ_4 = 1;
	KEY_COUNTER_READ_8 = 1;
	KEY_COUNTER_READ_16 = 1;
	KEY_COUNTER_READ_32 = 1;
	KEY_COUNTER_READ_64 = 1;
	KEY_COUNTER_READ_128 = 1;

	KEY_COUNTER_WRITE_4 = 1;
	KEY_COUNTER_WRITE_8 = 1;
	KEY_COUNTER_WRITE_16 = 1;
	KEY_COUNTER_WRITE_32 = 1;
	KEY_COUNTER_WRITE_64 = 1;
	KEY_COUNTER_WRITE_128 = 1;
}

#endif