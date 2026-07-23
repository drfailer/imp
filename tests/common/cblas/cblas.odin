package cblas

import "core:c"

foreign import openblas "libopenblas.a"

CBlasOrder :: enum c.int {RowMajor=101, ColMajor=102}
CBlasTranspose :: enum c.int {NoTrans=111, Trans=112, ConjTrans=113, ConjNoTrans=114}
blasint :: c.int

foreign openblas {

cblas_dgemm :: proc(Order: CBlasOrder, TransA, TransB: CBlasTranspose, M, N, K: blasint, alpha: f64, A: [^]f64, lda: blasint, B: [^]f64, ldb: blasint, beta: f64, C: [^]f64, ldc: blasint) ---
cblas_daxpy :: proc(N: blasint, alpha: f64, X: [^]f64, incX: blasint, Y: [^]f64, incY: blasint) ---
openblas_set_num_threads :: proc(count: int) ---

}

dgemm :: #force_inline proc(TransA, TransB: CBlasTranspose, M, N, K: uint, alpha: f64, A: [^]f64, lda: uint,
              B: [^]f64, ldb: uint, beta: f64, C: [^]f64, ldc: uint) {
    cblas_dgemm(.RowMajor, TransA, TransB, blasint(M), blasint(N), blasint(K), alpha,
                A, blasint(lda), B, blasint(ldb), beta, C,
                blasint(ldc))
}

daxpy :: #force_inline proc(N: uint, alpha: f64, X: [^]f64, incX: uint, Y: [^]f64, incY: uint) {
    cblas_daxpy(blasint(N), alpha, X, blasint(incX), Y, blasint(incY))
}
