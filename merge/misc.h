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

#define xrealloc(ptr, s) ({           \
	void *ret_ = realloc(ptr, s); \
	if (ret_ == NULL) {           \
		perror("realloc");    \
		exit(1);              \
	}                             \
	ret_;})

#define DIV_ROUNDUP(n,d) (((n) + (d) - 1) / (d))

#define dbg_print_str__ ">>>>>>>>>>>>>> %s() [%s +%d]"
#define dbg_print_arg__ __FUNCTION__, __FILE__, __LINE__
#define dbg_print(msg ,fmt, args...)\
    printf(dbg_print_str__ " " msg "\033[31m" fmt "\033[0m" , dbg_print_arg__ , ##args)

#define XDEBUG
#define msg(fmt,args...)      dbg_print("msg:",fmt, ##args)
#if defined(XDEBUG)
    #define dmsg(fmt,args...) dbg_print("dbg:",fmt, ##args)
#else
    #define dmsg(fmt,args...) do { } while (0)
#endif

#endif /* MISC_H__ */
