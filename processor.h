#ifndef PROCESSOR_H__
#define PROCESSOR_H__

#include <inttypes.h>

#if !defined(__i386__) && !defined(__x86_64__)
#error "This file is only for x86 ISAs"
#endif

/* Assume a cacheline size of 64 bytes  */
#define CACHELINE_BYTES 64

#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_BYTES)))

/*
 * Processor-specific functions
 */

/* Usefull stuff copied from linux kernel tree */

#define LOCK_PREFIX "lock ; "

/* hint the processor that this is a spin-loop */
static inline void relax_cpu(void)
{
	__asm__ __volatile__("rep; nop" ::: "memory");
}

/* Atomic type for performing atomic operations */
typedef struct {
	volatile int counter;
} atomic_t;

// arch/x86/include/asm/atomic_64.h

/*
 * Atomic operations that C can't guarantee us.  Useful for
 * resource counting etc..
 */

/**
 * atomic_read - read atomic variable
 * @v: pointer of type atomic_t
 *
 * Atomically reads the value of @v.
 */
#define atomic_read(v)      ((v)->counter)

/**
 * atomic_set - set atomic variable
 * @v: pointer of type atomic_t
 * @i: required value
 *
 * Atomically sets the value of @v to @i.
 */
#define atomic_set(v, i)        (((v)->counter) = (i))

/**
 * atomic_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type atomic_t
 *
 * Atomically adds @i to @v.
 */
static inline void atomic_add(int i, atomic_t *v)
{
	__asm__ __volatile__(LOCK_PREFIX "addl %1,%0"
		     : "=m" (v->counter)
		     : "ir" (i), "m" (v->counter));
}

/**
 * atomic_sub - subtract the atomic variable
 * @i: integer value to subtract
 * @v: pointer of type atomic_t
 *
 * Atomically subtracts @i from @v.
 */
static inline void atomic_sub(int i, atomic_t *v)
{
	__asm__ __volatile__(LOCK_PREFIX "subl %1,%0"
		     : "=m" (v->counter)
		     : "ir" (i), "m" (v->counter));
}

/**
 * atomic_sub_and_test - subtract value from variable and test result
 * @i: integer value to subtract
 * @v: pointer of type atomic_t
 *
 * Atomically subtracts @i from @v and returns
 * true if the result is zero, or false for all
 * other cases.
 */
static inline int atomic_sub_and_test(int i, atomic_t *v)
{
	unsigned char c;

	__asm__ __volatile__(LOCK_PREFIX "subl %2,%0; sete %1"
		     : "=m" (v->counter), "=qm" (c)
		     : "ir" (i), "m" (v->counter) : "memory");
	return c;
}

/**
 * atomic_inc - increment atomic variable
 * @v: pointer of type atomic_t
 *
 * Atomically increments @v by 1.
 */
static inline void atomic_inc(atomic_t *v)
{
	__asm__ __volatile__(LOCK_PREFIX "incl %0"
		     : "=m" (v->counter)
		     : "m" (v->counter));
}

/**
 * atomic_dec - decrement atomic variable
 * @v: pointer of type atomic_t
 *
 * Atomically decrements @v by 1.
 */
static inline void atomic_dec(atomic_t *v)
{
	__asm__ __volatile__(LOCK_PREFIX "decl %0"
		     : "=m" (v->counter)
		     : "m" (v->counter));
}

/**
 * atomic_add_return - add and return
 * @i: integer value to add
 * @v: pointer of type atomic_t
 *
 * Atomically adds @i to @v and returns @i + @v
 */
static inline int atomic_add_return(int i, atomic_t *v)
{
	int __i = i;
	__asm__ __volatile__(LOCK_PREFIX "xaddl %0, %1"
		     : "+r" (i), "+m" (v->counter)
		     : : "memory");
	return i + __i;
}

static inline int atomic_sub_return(int i, atomic_t *v)
{
	return atomic_add_return(-i, v);
}

#define atomic_inc_return(v)  (atomic_add_return(1, v))
#define atomic_dec_return(v)  (atomic_sub_return(1, v))

/*
 * do_loops: keep the processor busy, doing nothing
 */
static inline void do_loops(uint64_t cnt)
{
	uint64_t i=0;
	__asm__ __volatile__(
		"1:\n\t"
		"addq $1, %[idx]\n\t"
		"cmpq %[idx], %[cnt]\n\t"
		"ja 1b\n\t"

		: [idx] "=&r" (i)
		: "[idx]" (i), [cnt] "r" (cnt)
	);
}

#endif
