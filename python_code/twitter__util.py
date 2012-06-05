# -*- coding: utf-8 -*-

import sys
import locale
import weibo
import redis
import json
import time
from random import shuffle
from urllib2 import URLError
from twitter__login import login


def samplemapper(lst,sample):
    if sample < 1.0:
        shuffle(lst)
        return  lst[:int(len(lst) * sample)]
    else:
        return lst


def makeTwitterRequest(t, twitterFunction, max_errors=3, *args, **kwArgs):
    wait_period = 5
    error_count = 0
    while True:
        try:
            return twitterFunction(*args, **kwArgs)
        except weibo.api.TwitterHTTPError, e:
            error_count = 0
            wait_period = handleTwitterHTTPError(e, t, wait_period)
            if wait_period is None:
                return None
        except URLError, e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise

def _getRemainingHits(t):
    return t.account.rate_limit_status()['remaining_user_hits']

# Handle the common HTTPErrors. Return an updated value for wait_period
# if the problem is a 503 error. Block until the rate limit is reset if
# a rate limiting issue
def handleTwitterHTTPError(e, t, wait_period=2):

    if wait_period > 3600: # Seconds
        print >> sys.stderr, 'Too many retries. Quitting.'
        raise e

    r=json.loads(e.response_data)
    print r
    if (r.has_key('error_code')):
        if (r['error_code'] == 20003):
            print >> sys.stderr, 'error_code %i ,user not exist ' %(r['error_code'])
            return None
        elif (r['error_code'] in( 10022,10023,10024)):
            print >> sys.stderr, 'rate limit exhausted. '
            return None
        else:
            print >> sys.stderr, 'error_code %i' %(r['error_code'])
            return None 

    if e.e.code == 401:
        print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
        return None

    elif e.e.code == 403:
        print >> sys.stderr, 'Rate limit reached: sleeping for %i secs' % (sleep_time, )
        time.sleep(wait_period)
        wait_period *= 1.5
        return wait_period

    elif e.e.code in (502, 503):
        print >> sys.stderr, 'Encountered %i Error. Will retry in %i seconds' % (e.e.code,
                wait_period)
        time.sleep(wait_period)
        wait_period *= 1.5
        return wait_period

    else:
        raise e


# A template-like function that can get friends or followers depending on
# the function passed into it via func.

def _getFriendsOrFollowersUsingFunc(
    func,
    key_name,
    t, # Twitter connection
    r, # Redis connection
    screen_name=None,
    limit=10000,
    ):
    cursor = -1

    result = []
    while cursor != 0:
        response = makeTwitterRequest(t, func, screen_name=screen_name, cursor=cursor)
        for item in response['ids']:
            result.append(item)
            r.sadd(getRedisIdByScreenName(screen_name, key_name), item)

        cursor = response['next_cursor']
        scard = r.scard(getRedisIdByScreenName(screen_name, key_name))
        print >> sys.stderr, 'Fetched %s ids for %s' % (scard, screen_name)
        if scard >= limit:
            break
    return result

def _getSomeProfileInBatchFunc(
    func,
    profile, # users , status ,commmets et al
    key_name,
    t, # Twitter connection
    r, # Redis connection
    screen_name=None,
    uid=None,
    limit=10000,
    ):
    cursor = -1
    result = []
    scard=0
    while cursor != 0:
        response = makeTwitterRequest(t, func, screen_name=screen_name, cursor=cursor,count=200)
        if response is None:
            break
        for item in response[profile]:
            
            result.append(item)
            r.set(getRedisIdByScreenName(item['screen_name'], key_name),
                  json.dumps(item))
            r.set(getRedisIdByUserId(item['id'], key_name), 
                  json.dumps(item))
        cursor = response['next_cursor']
        scard+=len(response[profile])
        if scard >= limit:
            break
    return result

def getUserInfo(  # weibo dosenot suppoer batch query,it easily result rate limit exhausted
    t, # Twitter connection
    r, # Redis connection
    screen_names=[],
    user_ids=[],
    verbose=False,
    sample=1.0,
    ):

    # Sampling technique: randomize the lists and trim the length.

    if sample < 1.0:
        for lst in [screen_names, user_ids]:
            shuffle(lst)
            lst = lst[:int(len(lst) * sample)]

    info = []
    while len(screen_names) > 0:
        response = makeTwitterRequest(t,
                                      t.users.show,
                                      screen_name=screen_names[0])
        screen_names = screen_names[1:]
        if response is None:
            continue

        r.set(getRedisIdByScreenName(response['screen_name'], 'info.json'),
                  json.dumps(response))
        r.set(getRedisIdByUserId(response['id'], 'info.json'), 
                  json.dumps(response))
        info.extend([response])

    while len(user_ids) > 0:
        response = makeTwitterRequest(t, 
                                      t.users.show,
                                      uid=user_ids[0])
        user_ids = user_ids[1:]        
        if response is None:
            continue
        r.set(getRedisIdByScreenName(response['screen_name'], 'info.json'),
                  json.dumps(response))
        r.set(getRedisIdByUserId(response['id'], 'info.json'), 
                  json.dumps(response))
        info.extend([response])


    return info


# Convenience functions

def pp(_int):  # For nice number formatting
    locale.setlocale(locale.LC_ALL, '')
    return locale.format('%d', _int, True)


def getRedisIdByScreenName(screen_name, key_name):
    return 'screen_name$' + screen_name + '$' + key_name


def getRedisIdByUserId(user_id, key_name):
    return 'user_id$' + str(user_id) + '$' + key_name

def RedisUserId2UserInfo(r,user_id):
    info=(r.get(getRedisIdByUserId(user_id,'info.json')))
    if info is None:
        return None
    else:
        return json.loads(info)

if __name__ == '__main__': # For ad-hoc testing

    def makeTwitterRequest(t, twitterFunction, *args, **kwArgs): 
        wait_period = 2
        while True:
            try:
                e = Exception()
                e.code = 401
                #e.code = 502
                #e.code = 503
                raise weibo.api.TwitterHTTPError(e, "http://foo.com", "FOO", "BAR")
                return twitterFunction(*args, **kwArgs)
            except weibo.api.TwitterHTTPError, e:
                wait_period = handleTwitterHTTPError(e, t, wait_period)
                if wait_period is None:
                    return

    def _getRemainingHits(t):
        return 0

    t = login()
    makeTwitterRequest(t, t.friends.ids, screen_names=['SocialWebMining'])
