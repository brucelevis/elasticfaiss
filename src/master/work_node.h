/*
 * data_nodes.h
 *
 *  Created on: 2018Äê7ÔÂ13ÈÕ
 *      Author: qiyingwang
 */

#ifndef SRC_MASTER_DATA_NODES_H_
#define SRC_MASTER_DATA_NODES_H_

#include <string>
#include <unordered_map>
#include <stdint.h>

namespace elasticfaiss
{

    class WorkNodeManager
    {
        private:

        public:
            void add_node();
            int touch_node();
            int load_snapshot(const std::string& file);
            int save_snapshot(const std::string& file);

    };
}

#endif /* SRC_MASTER_DATA_NODES_H_ */
