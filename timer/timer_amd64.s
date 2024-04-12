#include "textflag.h"

// func cputicks() int64
TEXT ·Rdtscp(SB),NOSPLIT,$0-0
	RDTSCP
	SHLQ	$32, DX
	ADDQ	DX, AX
	MOVQ	AX, ret+0(FP)
	RET
