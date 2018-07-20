/*
 * proto_helper.h
 *
 *  Created on: 2018年7月17日
 *      Author: qiyingwang
 */

#ifndef SRC_COMMON_PROTO_HELPER_H_
#define SRC_COMMON_PROTO_HELPER_H_

#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>

namespace elasticfaiss
{

    class ReqResProtoHolder
    {
        public:
            virtual const ::google::protobuf::Message* get_request() = 0;
            virtual ::google::protobuf::Message* get_response() = 0;
            virtual ~ReqResProtoHolder()
            {
            }
    };

    template<typename REQ, typename RES>
    class MessagePairClosure: public ::google::protobuf::Closure
    {
        public:
            REQ req;
            RES res;
        public:
            MessagePairClosure()
            {

            }
            void Run()
            {
                delete this;
            }
            ~MessagePairClosure()
            {
            }
    };
}

#endif /* SRC_COMMON_PROTO_HELPER_H_ */
