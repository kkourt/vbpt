#ifndef MISC_H__
#define MISC_H__

/* various helpers */

#if defined(__linux__)
	#include <pthread.h>
	/* pthread spinlock wrappers */
	#define spinlock_t       pthread_spinlock_t
	#define spinlock_init(x) pthread_spin_init(x, 0)
	#define spin_lock(x)     pthread_spin_lock(x)
	#define spin_unlock(x)   pthread_spin_unlock(x)
#elif defined(__APPLE__)
	#include <libkern/OSAtomic.h>
	#define spinlock_t OSSpinLock
	#define spinlock_init(x) do { *(x) = 0; } while (0)
	#define spin_lock(x) OSSpinLockLock(x)
	#define spin_unlock(x) OSSpinLockUnlock(x)
#endif

#include <stdlib.h> // malloc
#include <stdio.h> // perror

#define xmalloc(s) ({              \
	void *ret_ = malloc(s);    \
	if (ret_ == NULL) {        \
		perror("malloc");  \
		exit(1);           \
	}                          \
	ret_;})

#endif
