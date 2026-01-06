#ifndef VAR_KEY_H
#define VAR_KEY_H

#include <stdlib.h>

/* VARIABLE KEY SIZE FUNCTIONS AND VARIABLES */

extern double KEY_DISTRIBUTION_STATUS;
extern double SMALL_KEY_COUNTER;
extern double LARGE_KEY_COUNTER;

// 4 to 255 bytes
static inline int get_kv_key_size(double kd){

	// small keys = 4 ~ 32 bytes
	// large keys = 33 ~ 255 bytes

	int k;

    printf("Starting...\n");

	if(KEY_DISTRIBUTION_STATUS < kd){
        printf("pre calc...\n");
		LARGE_KEY_COUNTER++;
		k = (4 + rand() % (254 - 33 + 1));
        printf("after calc...\n");
	}
	else{
		SMALL_KEY_COUNTER++;
		k = (4 + rand() % (32 - 4 + 1));
	}

    printf("check dist...\n");
	if(SMALL_KEY_COUNTER == 0)
		KEY_DISTRIBUTION_STATUS = 1;
	else
		KEY_DISTRIBUTION_STATUS = LARGE_KEY_COUNTER / SMALL_KEY_COUNTER;

    printf("done...\n");
	return k;
}

#endif