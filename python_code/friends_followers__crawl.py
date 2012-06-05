# -*- coding: utf-8 -*-

import sys
import redis
import functools
from twitter__login import login
from twitter__util import getUserInfo
from twitter__util import _getSomeProfileInBatchFunc
from twitter__util import getRedisIdByUserId
from twitter__util import getRedisIdByScreenName
from twitter__util import samplemapper
from twitter__util import RedisUserId2UserInfo
import json

t = login()
r = redis.Redis()

# Some wrappers around _getSomeProfileInBatchFunc that 
# create convenience functions
getFriendsBatch = functools.partial(_getSomeProfileInBatchFunc,
                                    t.friendships.friends,'users', 'info.json', t, r)
getFollowersBatch = functools.partial(_getSomeProfileInBatchFunc,
                                      t.friendships.followers, 'users','info.json', t,r)
RedisUserId2UserInfoWraper = functools.partial(RedisUserId2UserInfo,r)
def flat(l):
    return reduce(lambda x,y:x+y, l)


def crawl(
    screen_names=[],
    friends_limit=10000,
    followers_limit=10000,
    depth=1,
    friends_sample=0.2, #XXX
    followers_sample=0.0,
    ):
    
    def crawlmapper(screen_name):
        if  r.get(getRedisIdByScreenName(screen_name,'crawled_in_60min')) is None:
            r.set(getRedisIdByScreenName(screen_name,'crawled_in_60min'),'1')
            r.expire(getRedisIdByScreenName(screen_name,'crawled_in_60min'),3600)

            friends_info = getFriendsBatch(screen_name,friends_limit)
            map(lambda x:
                    r.sadd(getRedisIdByScreenName(screen_name, 'friend_ids'), 
                           x['id']),
                friends_info)
            scard = r.scard(getRedisIdByScreenName(screen_name, 'friend_ids'))
            print >> sys.stderr, 'Fetched %s ids for %s' % (scard, screen_name)

            
            followers_info = getFollowersBatch(screen_name,followers_limit)
            map(lambda x:
                    r.sadd(getRedisIdByScreenName(screen_name, 'follower_ids'),
                           x['id']),
                followers_info)
            scard = r.scard(getRedisIdByScreenName(screen_name, 'follower_ids'))
            print >> sys.stderr, 'Fetched %s ids for %s' % (scard, screen_name)
        else:
            friends_info=map(RedisUserId2UserInfoWraper,
                             list(r.smembers(getRedisIdByScreenName(screen_name,'friend_ids'))))
            followers_info=map(RedisUserId2UserInfoWraper,
                               list(r.smembers(getRedisIdByScreenName(screen_name,'follower_ids'))))
        
        return map(lambda u1: u1['screen_name'],
                   filter(lambda info:
                              (info['followers_count']<1000 and
                               info['friends_count']<1000), #filter Public Intellectual and Zombie
                          flat(map(samplemapper,
                                   [friends_info,followers_info],
                                   [friends_sample,followers_sample]))))
    
    getUserInfo(t, r, screen_names=screen_names)
    d=0
    while d<depth:
        d+=1
        screen_names=flat(map(crawlmapper,screen_names))
        print 'crawed ',len(screen_names) ,'ids'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Please supply at least one screen name."
    else:
        SCREEN_NAME = sys.argv[1]
        crawl([SCREEN_NAME])

        # The data is now in the system. Do something interesting. For example, 
        # find someone's most popular followers as an indiactor of potential influence.
        # See friends_followers__calculate_avg_influence_of_followers.py
