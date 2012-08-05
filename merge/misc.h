#ifndef MISC_H__
#define MISC_H__

/* various helpers */

#if defined(__linux__)
	/* pthread spinlock wrappers */

	#include <stdbool.h>
	#include <pthread.h>
	#include <errno.h> /* EBUSY */
	#define spinlock_t       pthread_spinlock_t

	static inline void
	spinlock_init(spinlock_t *lock)
	{
		int err __attribute__((unused)) = pthread_spin_init(lock, 0);
		assert(!err);
	}

	static inline void
	spin_lock(spinlock_t *lock)
	{
		int err __attribute__((unused)) = pthread_spin_lock(lock);
		assert(!err);
	}

	static inline void
	spin_unlock(spinlock_t *lock)
	{
		int err __attribute__((unused)) = pthread_spin_unlock(lock);
		assert(!err);
	}

	static inline bool
	spin_try_lock(spinlock_t *lock)
	{
		int err = pthread_spin_trylock(lock);
		if (err == EBUSY)
			return false;
		assert(!err);
		return true;
	}
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

#define dbg_print_str__ "%4d>>>>> %s() [%s +%d]"
#define dbg_print_arg__ gettid(), __FUNCTION__, __FILE__, __LINE__
#define dbg_print(msg ,fmt, args...)\
    printf(dbg_print_str__ " " msg "\033[31m" fmt "\033[0m" , dbg_print_arg__ , ##args)

#define XDEBUG
#define msg(fmt,args...)      dbg_print("msg:",fmt, ##args)
#if defined(XDEBUG)
    #define dmsg(fmt,args...) dbg_print("dbg:",fmt, ##args)
#else
    #define dmsg(fmt,args...) do { } while (0)
#endif

// gettid
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
static inline pid_t gettid(void)
{
	return syscall(SYS_gettid);
}

#define tmsg(fmt, args...) do { printf("%4d> " fmt, gettid(), ##args); } while (0)

#endif /* MISC_H__ */
