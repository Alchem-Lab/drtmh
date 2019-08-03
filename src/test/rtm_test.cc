#include <immintrin.h>
int main() {
     int n_tries, max_tries;
     unsigned status = _XABORT_EXPLICIT;
     
     for (n_tries = 0; n_tries < max_tries; n_tries++)
       {
         status = _xbegin ();
         if (status == _XBEGIN_STARTED || !(status & _XABORT_RETRY))
           break;
       }
     if (status == _XBEGIN_STARTED)
       {
         _xend ();
       }
     else
       {
       }

     return 0;
}
