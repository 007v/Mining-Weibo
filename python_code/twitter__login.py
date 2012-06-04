# -*- coding: utf-8 -*-

import os
import weibo
from weibo.oauth import write_token_file,read_token_file
from weibo.oauth_dance import oauth_dance

from configfile import * 

def login():

    # Go to http://twitter.com/apps/new to create an app and get these items
    # See also http://dev.twitter.com/pages/oauth_single_token

    TOKEN_FILE = 'out/weibo.oauth'

    try:
        access_token = read_token_file(TOKEN_FILE)
    except IOError, e:
        access_token = oauth_dance(WB_APP_KEY,WB_APP_SECRET,CALLBACK_URL,username,passwd)

        if not os.path.isdir('out'):
            os.mkdir('out')

        write_token_file(TOKEN_FILE, access_token)
         
    return weibo.Twitter(domain='api.weibo.com', api_version='2',
                        auth=weibo.oauth.OAuth(access_token,WB_APP_KEY))

if __name__ == '__main__':
    login()
