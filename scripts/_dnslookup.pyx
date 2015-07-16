from libc.stdlib cimport calloc, free

cdef extern from "sys/types.h":
    ctypedef int socklen_t
    ctypedef int sa_family_t

cdef extern from "sys/socket.h":
    cdef struct sockaddr

    enum: AF_INET
    enum: SOCK_STREAM

cdef extern from "netdb.h" nogil:

    cdef struct sigevent

    enum: GAI_WAIT
    enum: GAI_NOWAIT
    enum: NI_MAXHOST
    enum: NI_NUMERICHOST

    cdef struct addrinfo:
        int        ai_flags
        int        ai_family
        int        ai_socktype
        int        ai_protocol
        socklen_t  ai_addrlen
        sockaddr  *ai_addr
        char      *ai_canonname
        addrinfo  *ai_next

    cdef struct gaicb:
        const char *ar_name
        const char *ar_service
        addrinfo   *ar_request
        addrinfo   *ar_result

    int getaddrinfo_a(int mode, gaicb *items[], int nitems,
                      sigevent *sevp)
    int gai_error(gaicb *req)
    const char *gai_strerror(int err)

    int getnameinfo(const sockaddr *sa, socklen_t salen,
                    char *host, socklen_t hostlen,
                    char *serv, socklen_t servlen,
                    int flags)

    void freeaddrinfo(addrinfo *res)

from socket import gaierror as gai_exception
cdef object make_gaierror(int err):
    # The socket.gaierror constructor should do this, but it doesn't.
    return gai_exception(err, (<bytes>gai_strerror(err)).decode("ascii"))

cpdef list getaddrinfo_batch(list names):
    for n in names:
        if not isinstance(n, bytes):
            raise TypeError("all entries in 'names' must be byte strings")

    cdef int nitems = len(names)
    cdef gaicb *gai_vec   = NULL
    cdef gaicb **gai_dvec = NULL
    cdef char host[NI_MAXHOST]
    cdef addrinfo hints
    cdef list result

    hints.ai_flags     = 0
    hints.ai_family    = AF_INET
    hints.ai_socktype  = SOCK_STREAM
    hints.ai_protocol  = 0
    hints.ai_addrlen   = 0
    hints.ai_addr      = NULL
    hints.ai_canonname = NULL
    hints.ai_next      = NULL

    try:
        gai_vec  = <gaicb *>calloc(nitems, sizeof(gaicb))
        gai_dvec = <gaicb **>calloc(nitems, sizeof(gaicb *))

        if not gai_vec or not gai_dvec:
            raise MemoryError()

        for i, name in enumerate(names):
            gai_dvec[i]           = &gai_vec[i]
            gai_vec[i].ar_name    = <bytes>name
            gai_vec[i].ar_service = NULL
            gai_vec[i].ar_request = &hints
            gai_vec[i].ar_result  = NULL

        with nogil:
            ret = getaddrinfo_a(GAI_WAIT, gai_dvec, nitems, NULL)

        if ret:
            raise make_gaierror(ret)

        result = []
        for i, name in enumerate(names):
            ret = gai_error(gai_dvec[i])
            if ret:
                result.append((name, make_gaierror(ret)))
            else:
                addrs = []
                res = gai_vec[i].ar_result
                while res:
                    ret = getnameinfo(res.ai_addr, res.ai_addrlen,
                                      host, NI_MAXHOST,
                                      NULL, 0, NI_NUMERICHOST)
                    # Just discard entries for which getnameinfo fails.
                    if ret == 0:
                        addrs.append(<bytes>host)
                    res = res.ai_next

                result.append((name, addrs))

        return result

    finally:
        if gai_vec is not NULL:
            for i in range(nitems):
                freeaddrinfo(gai_vec[i].ar_result)
        free(gai_vec)
        free(gai_dvec)
