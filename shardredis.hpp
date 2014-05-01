#ifndef  SHARDREDIS_HPP
#define  SHARDREDIS_HPP

#include "hiredis.h"
#include "crc32.hpp"
#include <map>
#include <vector>
#include <string>
#include <string.h>
#include <stdexcept>

struct RedisConfig{
	std::string hostname;
	int port;
	int db;
};

class ShardRedis {
public:
	ShardRedis(std::vector<RedisConfig> configs) {

		//loop configs
		int count = 0;
		for (std::vector<RedisConfig>::iterator it = configs.begin(); it != configs.end(); ++it) {

			//connect
			RedisConfig myconf = *it;
			redisContext *c = redisConnect(myconf.hostname.c_str(), myconf.port);
			if (c != NULL && c->err) {
				// handle error
				throw std::runtime_error(c->errstr);
			}

			//select db
			redisReply* reply = (redisReply*) redisCommand(c, "SELECT %u", myconf.db);
			freeReplyObject(reply);

			//save
			_connections[count] = c;
			count++;

		}

	}

	~ShardRedis(){

		for(int i = 0 ; i < _connections.size() ; i++)
			redisFree(_connections[i]);

	}

	redisContext* getShard(const char * key) {

		//crc32 on key
		uint32_t crc = CCrc32::Calc(key, strlen(key));

		//return RedisContext for key
		return _connections[crc % _connections.size()];
	}

private:
	std::map<int, redisContext *> _connections;
};

#endif /* SHARDREDIS_HPP */
