#include <stdio.h>
#include <string.h>
#include <alloca.h>
#include <sys/types.h>
#include <stdlib.h>
#include <pthread.h>

typedef int (*comparer)(const void *, const void *);

#define THREAD_THRESHOLD 8192

void _qsort (size_t element_size, void *arr, int left, int right, comparer cmp);
void _qsort_8 (void *arr, int left, int right, comparer cmp);
void _qsort_4 (void *arr, int left, int right, comparer cmp);
void _qsort_2 (void *arr, int left, int right, comparer cmp);
void _qsort_1 (void *arr, int left, int right, comparer cmp);

struct qsort_args {
  size_t element_size;
  void *arr;
  int left;
  int right;
  comparer cmp;
};

struct qsort_args _qsort_args (size_t element_size, void *arr, int left, int right, comparer cmp)
{
  struct qsort_args qa;
  qa.element_size = element_size;
  qa.arr = arr;
  qa.left = left;
  qa.right = right;
  qa.cmp = cmp;
  return qa;
}

void *_qsort_start (void *args)
{
  struct qsort_args *a = (struct qsort_args *)args;
  _qsort (a->element_size, a->arr, a->left, a->right, a->cmp);
  return NULL;
}
void *_qsort_8_start (void *args)
{
  struct qsort_args *a = (struct qsort_args *)args;
  _qsort_8 (a->arr, a->left, a->right, a->cmp);
  return NULL;
}
void *_qsort_4_start (void *args)
{
  struct qsort_args *a = (struct qsort_args *)args;
  _qsort_4 (a->arr, a->left, a->right, a->cmp);
  return NULL;
}
void *_qsort_2_start (void *args)
{
  struct qsort_args *a = (struct qsort_args *)args;
  _qsort_2 (a->arr, a->left, a->right, a->cmp);
  return NULL;
}
void *_qsort_1_start (void *args)
{
  struct qsort_args *a = (struct qsort_args *)args;
  _qsort_1 (a->arr, a->left, a->right, a->cmp);
  return NULL;
}



inline size_t average2 (size_t a, size_t b)
{
  return ((a ^ b) >> 1) + (a & b);
}

void swap (size_t sz, void *arr, size_t i, size_t j)
{
  char *a = (char*)(arr + i*sz);
  char *b = (char*)(arr + j*sz);
  long tmp;
  for (i=0; i<sz-sz%sizeof(long); i+=sizeof(long)) {
    tmp = *(long*)&a[i];
    *(long*)&a[i] = *(long*)&b[i];
    *(long*)&b[i] = tmp;
  }
  for (; i<sz; i++) {
    tmp = a[i];
    a[i] = b[i];
    b[i] = (char)tmp;
  }
}

inline void swap_8 (void *arr, size_t i, size_t j)
{
  int64_t *a = (int64_t*)arr + i;
  int64_t *b = (int64_t*)arr + j;
  int64_t tmp;
  tmp = *a;
  *a = *b;
  *b = tmp;
}

inline void swap_4 (void *arr, size_t i, size_t j)
{
  int32_t *a = (int32_t*)arr + i;
  int32_t *b = (int32_t*)arr + j;
  int32_t tmp;
  tmp = *a;
  *a = *b;
  *b = tmp;
}

inline void swap_2 (void *arr, size_t i, size_t j)
{
  int16_t *a = (int16_t*)arr + i;
  int16_t *b = (int16_t*)arr + j;
  int16_t tmp;
  tmp = *a;
  *a = *b;
  *b = tmp;
}

inline void swap_1 (void *arr, size_t i, size_t j)
{
  char tmp;
  tmp = ((char*)arr)[i];
  ((char*)arr)[i] = ((char*)arr)[j];
  ((char*)arr)[j] = tmp;
}

void _qsort (size_t element_size, void *arr, int left, int right, comparer cmp)
{
  int i, last;
  pthread_t lt;
  struct qsort_args qa;

  if (left >= right)
    return;
  swap(element_size, arr, left, average2(left, right));
  last = left;
  for (i = left + 1; i <= right; i++) {
    if (cmp(arr + i*element_size, arr + left*element_size) < 0) {
      last++;
      swap(element_size, arr, last, i);
    }
  }
  swap (element_size, arr, left, last);
  if (right-left > THREAD_THRESHOLD) {
    qa = _qsort_args(element_size, arr,left,last-1,cmp);
    if (0 == pthread_create(&lt, PTHREAD_CREATE_JOINABLE, _qsort_start, (void*)&qa)) {
      _qsort (element_size, arr, last+1, right, cmp);
      pthread_join(lt, NULL);
      return;
    }
  }
  _qsort (element_size, arr, left, last-1, cmp);
  _qsort (element_size, arr, last+1, right, cmp);
}

void _qsort_8 (void *arr, int left, int right, comparer cmp)
{
  int i, last;
  pthread_t lt;
  struct qsort_args qa;

  if (left >= right)
    return;
  swap_8 (arr, left, average2(left, right));
  last = left;
  for (i = left + 1; i <= right; i++) {
    if (cmp(arr + i*8, arr + left*8) < 0) {
      last++;
      swap_8 (arr, last, i);
    }
  }
  swap_8 (arr, left, last);
  if (right-left > THREAD_THRESHOLD) {
    qa = _qsort_args(8, arr,left,last-1,cmp);
    if (0 == pthread_create(&lt, PTHREAD_CREATE_JOINABLE, _qsort_8_start, (void*)&qa)) {
      _qsort_8 (arr, last+1, right, cmp);
      pthread_join(lt, NULL);
      return;
    }
  }
  _qsort_8 (arr, left, last-1, cmp);
  _qsort_8 (arr, last+1, right, cmp);
}


void _qsort_4 (void *arr, int left, int right, comparer cmp)
{
  int i, last;
  pthread_t lt;
  struct qsort_args qa;

  if (left >= right)
    return;
  swap_4 (arr, left, average2(left, right));
  last = left;
  for (i = left + 1; i <= right; i++) {
    if (cmp(arr + i*4, arr + left*4) < 0) {
      last++;
      swap_4 (arr, last, i);
    }
  }
  swap_4 (arr, left, last);
  if (right-left > THREAD_THRESHOLD) {
    qa = _qsort_args(4, arr,left,last-1,cmp);
    if (0 == pthread_create(&lt, PTHREAD_CREATE_JOINABLE, _qsort_4_start, (void*)&qa)) {
      _qsort_4 (arr, last+1, right, cmp);
      pthread_join(lt, NULL);
      return;
    }
  }
  _qsort_4 (arr, left, last-1, cmp);
  _qsort_4 (arr, last+1, right, cmp);
}

void _qsort_2 (void *arr, int left, int right, comparer cmp)
{
  int i, last;
  pthread_t lt;
  struct qsort_args qa;

  if (left >= right)
    return;
  swap_2 (arr, left, average2(left, right));
  last = left;
  for (i = left + 1; i <= right; i++) {
    if (cmp(arr + i*2, arr + left*2) < 0) {
      last++;
      swap_2 (arr, last, i);
    }
  }
  swap_2 (arr, left, last);
  if (right-left > THREAD_THRESHOLD) {
    qa = _qsort_args(2, arr,left,last-1,cmp);
    if (0 == pthread_create(&lt, PTHREAD_CREATE_JOINABLE, _qsort_2_start, (void*)&qa)) {
      _qsort_2 (arr, last+1, right, cmp);
      pthread_join(lt, NULL);
      return;
    }
  }
  _qsort_2 (arr, left, last-1, cmp);
  _qsort_2 (arr, last+1, right, cmp);
}

void _qsort_1 (void *arr, int left, int right, comparer cmp)
{
  int i, last;
  pthread_t lt;
  struct qsort_args qa;

  if (left >= right)
    return;
  swap_1(arr, left, average2(left, right));
  last = left;
  for (i = left + 1; i <= right; i++) {
    if (cmp(arr + i, arr + left) < 0) {
      last++;
      swap_1(arr, last, i);
    }
  }
  swap_1 (arr, left, last);
  if (right-left > THREAD_THRESHOLD) {
    qa = _qsort_args(1, arr,left,last-1,cmp);
    if (0 == pthread_create(&lt, PTHREAD_CREATE_JOINABLE, _qsort_1_start, (void*)&qa)) {
      _qsort_1 (arr, last+1, right, cmp);
      pthread_join(lt, NULL);
      return;
    }
  }
  _qsort_1 (arr, left, last-1, cmp);
  _qsort_1 (arr, last+1, right, cmp);
}

void* my_qsort (void *arr, size_t length, size_t element_size, comparer cmp)
{
//   printf("qsort %d [%d] size %d\n", arr, length, element_size);
  if (element_size == 1)
    _qsort_1 (arr, 0, length-1, cmp);
  else if (element_size == 2)
    _qsort_2 (arr, 0, length-1, cmp);
  else if (element_size == 4)
    _qsort_4 (arr, 0, length-1, cmp);
  else if (element_size == 8)
    _qsort_8 (arr, 0, length-1, cmp);
  else
    _qsort (element_size, arr, 0, length-1, cmp);
  return arr;
}

inline int charcmp (const void *a, const void *b)
{
  return (*(char*)a) - (*(char*)b);
}

inline int intcmp (const void *a, const void *b)
{
  return (*(int32_t*)a) - (*(int32_t*)b);
}

void printsort (char *s)
{
  char *d = (char*)alloca(strlen(s)+1);
  strcpy(d,s);
  my_qsort(d, strlen(d), 1, charcmp);
  printf("%s -> %s\n", s, d);
}

void test_string ()
{
  printsort("foobarbeque");
  printsort("abcdef");
  printsort("fedcba");
  printsort("abc");
  printsort("acb");
  printsort("bac");
  printsort("bca");
  printsort("cab");
  printsort("cba");
  printsort("ab");
  printsort("ba");
  printsort("a");
  printsort("");
}

void test_int32 ()
{
  int32_t arr[12], i;
  for (i=0; i<12; i++) arr[i] = 10-i;
  my_qsort(arr, 12, sizeof(int32_t), intcmp);
  for (i=0; i<12; i++)
    printf("%d ", arr[i]);
  printf("\n");
}

void test_int64 ()
{
  int64_t arr[12], i;
  for (i=0; i<12; i++) arr[i] = 10-i;
  my_qsort(arr, 12, sizeof(int64_t), intcmp);
  for (i=0; i<12; i++)
    printf("%d ", arr[i]);
  printf("\n");
}

void benchmark_int64 ()
{
  srand (0);
  int64_t *arr, i;
  arr = (int64_t*)malloc(10000000*sizeof(int64_t));
  for (i=0; i<10000000; i++) arr[i] = rand();
  my_qsort(arr, 10000000, sizeof(int64_t), intcmp);
}

int main (int argc, char* argv[])
{
  test_string ();
  test_int32 ();
  test_int64 ();
  benchmark_int64 ();
  return 0;
}