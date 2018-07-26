/*
 * helper.cpp
 *
 *  Created on: 2018年7月25日
 *      Author: qiyingwang
 */

#include "helper.h"
#include <bthread/bthread.h>
namespace elasticfaiss
{
    static void* func_executor(void* arg)
    {
        AsyncFunc* func = (AsyncFunc*)arg;
        (*func)();
        delete func;
        return NULL;
    }

    void start_bthread_function(const AsyncFunc& func)
    {
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, func_executor, new AsyncFunc(func));
    }
}

