/*
 * elasticfaiss.h
 *
 *  Created on: 2018年7月18日
 *      Author: qiyingwang
 */

#ifndef SRC_COMMON_ELASTICFAISS_H_
#define SRC_COMMON_ELASTICFAISS_H_
#include <string>
#include <set>
#include <butil/endpoint.h>
#include <stdint.h>
#include "helper.h"

namespace elasticfaiss
{
    extern butil::EndPoint g_listen_endpoint;
    extern std::string g_home;
    extern std::string g_listen;
    extern std::string g_masters;
    extern std::string g_master_group;
    extern int32_t g_election_timeout_ms;
    extern int32_t g_node_snapshot_interval;
    extern int32_t g_master_snapshot_interval;
    typedef std::set<std::string> StringSet;
}

#endif /* SRC_COMMON_ELASTICFAISS_H_ */
