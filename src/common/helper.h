/*
 * helper.h
 *
 *  Created on: 2018年7月19日
 *      Author: qiyingwang
 */

#ifndef SRC_COMMON_HELPER_H_
#define SRC_COMMON_HELPER_H_

#include <string>
#include <sstream>
#include <functional>

namespace elasticfaiss
{
    template<typename T>
    std::string string_join_container(const T& container, const std::string& sep)
    {
        typename T::const_iterator it = container.begin();
        std::stringstream buffer;
        while (it != container.end())
        {
            buffer<< (*it);
            if (*it != *(container.rbegin()))
            {
                buffer << sep;
            }
            it++;
        }
        return buffer.str();
    }
    typedef std::function<void(void)> AsyncFunc;

    void start_bthread_function(const AsyncFunc& func);
}



#endif /* SRC_COMMON_HELPER_H_ */
