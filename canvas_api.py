##############################################################################
#
#
#
#
#
#        Canvas API
#           
#           Ji Guo
#
#
#
#
#
##############################################################################


import multiprocessing
from threading import Thread
import pandas as pd
import numpy as np
import requests
import json
import html
from datetime import datetime
from urllib.parse import quote
from datetime import date
from dateutil import tz
import pickle
import os
import random


s###############################################################################
# Load data servers
###############################################################################

current_time = datetime.now() - timedelta(days = 2)
snapshot_time = current_time.strftime("%Y%m%d")


current_date = datetime.now() - timedelta(days = 1)
yesterday_date = current_date.strftime("%Y-%m-%d")
today_date = datetime.now().strftime("%Y-%m-%d")



# zulu time conversion
# input dataframe and timestamp variable name
def time_conversion(df, variable):
    """
    
    Parameters
    ----------
    df : dataframe
        the data frame .
    variable : string
        the column name.

    Returns
    -------
    data frame 

    """
    # convert to array
    df_array = df.to_numpy()
    # get the column id
    cid = df.columns.get_loc(variable)
    # from time zone
    from_zone = tz.tzutc()
    # to time zone
    to_zone = tz.tzlocal()
    # loop the array
    for i in range(len(df_array)):
        # get the time
        ms = df_array[i][cid]
        # convert to datetime type
        utc_ms= datetime.strptime(ms, '%Y-%m-%dT%H:%M:%SZ')
        # convert timezone
        c_ms = utc_ms.replace(tzinfo=from_zone).astimezone(to_zone)
        # replace the time without timezone
        df_array[i][cid] = c_ms.replace(tzinfo=None)
    # return final dataframe with column names        
    final = pd.DataFrame(df_array, columns = df.columns)
    return final


###############################################################################
#
#
# GET CANVAS DATA
#
#
###############################################################################

# get course enrollment
def get_enrollment(canvas_course_id, school, email = False, tokens):
    
    """
    

    Parameters
    ----------
    canvas_course_id : string
        DESCRIPTION.
    school : string
        the name of the school for canvas api.
    email : optional
        The default is False.
    tokens: list
        the list of tokens

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    print('Pull enrollment data from API for course ', canvas_course_id)
    #randomly pick a token from the token list
    token = {"Authorization": """Bearer {}""".format(random.choice(tokens))}
    #setup parameters
    pts = {'per_page': 100,
           'state[]':['active', 'invited', 'creation_pending', 'rejected', 'completed', 'inactive', 'current_and_invited', 'current_and_future', 'current_and_concluded'],
           'include[]':'avatar_url'}
    #get the initial response
    initial_response = requests.get("""https://{}.instructure.com/api/v1/courses/{}/enrollments""".format(school, canvas_course_id), params = pts, headers = token)
    # create a list
    data = []
    while True:
        # get the raw json
        # append the json, 
        # if next url == next, continue, else break
        raw = json.loads(initial_response.text)
        for i in raw:
            data.append(i)
        try:
            initial_response.links['next']['rel'] == 'next'
            new_link = initial_response.links['next']['url']
            new_response = requests.get(new_link, params = pts, headers = token)
            initial_response = new_response
        except KeyError:
            break
    # create lists 
    course_id = []
    course_section_id = []
    user_id = []
    login_id = []
    university_id = []
    name = []
    enrollment_state = []
    last_activity_at = []
    role = []
    total_activity_time = []
    avatar = []
    
    # loop the data to get the json list
    for access in data:
        course_id.append(access['course_id'])
        course_section_id.append(access['course_section_id'])
        user_id.append(access['user_id'])
        login_id.append(access['user']['login_id'])
        university_id.append(access['user']['sis_user_id'])
        name.append(access['user']['name'])
        enrollment_state.append(access['enrollment_state'])
        last_activity_at.append(access['last_activity_at'])
        role.append(access['role'])
        total_activity_time.append(access['total_activity_time'])
        if 'https://{}.instructure.com/images/messages/avatar-50.png'.format(school) in access['user']['avatar_url']:
            avatar.append('No')
        else:
            avatar.append('Yes')
    # create the final dataset 
    final_data = pd.DataFrame({'canvas_course_id':course_id,
                               'canvas_section_id':course_section_id,
                               'canvas_user_id':user_id,
                               'hawk_id':login_id,
                               'university_id':university_id,
                               'user_name':name,
                               'status':enrollment_state,
                               'last_activity_time':last_activity_at,
                               'role':role,
                               'total_activity_time':total_activity_time,
                               'avatar':avatar})
    final_data = final_data[final_data.user_name != 'Test Student'].drop_duplicates('canvas_user_id').reset_index(drop = True)

    # if email = TRUE, get emails from the api
    if email == True:
        #get emails
        user_data = []
        for i in range(len(final_data)):
            canvas_user_id = final_data.canvas_user_id[i]
            initial_response = requests.get("""https://{}.instructure.com/api/v1/users/{}""".format(school, canvas_user_id), params = pts, headers = token)
            raw = json.loads(initial_response.text)
            user_data.append(raw)
    
        cid = []
        uid = []
        email1 = []
        email2 = []
        
        for i in range(len(user_data)):
            cid.append(user_data[i]['id'])
            uid.append(user_data[i]['sis_user_id'])
            email1.append(user_data[i]['email'])
            email2.append(user_data[i]['login_id'] + '@uiowa.edu')
            
        emails = pd.DataFrame({'canvas_user_id':cid,
                               'university_id':uid,
                               'email1':email1,
                               'email2':email2})
        # merge the data with the email data by canvas user id
        final_email = pd.merge(final_data, emails).drop_duplicates('canvas_user_id').reset_index(drop = True)
        return final_email
    else:
        return final_data



###############################################################################
# API to get the requests
###############################################################################

#get student's pageviews
def get_pageviews(user_id, school, start_date, end_date, tokens):
    """
    

    Parameters
    ----------
    user_id : string
        canvas user id.
    school : string
        school name.
    start_date : string
        start time in %Y-%m-%d format.
    end_date : string 
        end time in %Y-%m-%d format.
    tokens : list
        the list of tokens.

    Returns
    -------
    data : TYPE
        DESCRIPTION.

    """

    #randomly pick a token from a list
    token = {"Authorization": """Bearer {}""".format(random.choice(tokens))}
    #setup parameters
    pts = {'per_page': 100,
           'start_time': start_date,
           'end_time': end_date}
    #get the initial response
    initial_response = requests.get("""https://{}.instructure.com/api/v1/users/{}/page_views""".format(school, user_id), params = pts, headers = token)
    data = []
    while True:
        raw = json.loads(initial_response.text)
        for i in raw:
            data.append(i)
        try:
            initial_response.links['next']['rel'] == 'next'
            new_link = initial_response.links['next']['url']
            new_response = requests.get(new_link, params = pts, headers = token)
            initial_response = new_response
        except KeyError:
            break
    return data


###############################################################################
# Convert data from get_pageviews
###############################################################################

def convert_data(df, canvas_course_id):
    """

    Parameters
    ----------
    df : list
        the list comes from pageviews.
    canvas_course_id : string
        canvas course id.

    Returns
    -------
    dataframe.

    """
    

    new_data= []
    for i in range(len(data)):
        for j in data[i]:
            new_data.append(j) 
            
    # create lists
    session_id = []
    url = []
    context_type = []
    asset_type = []
    controller = []
    action = []
    interaction_seconds = []
    created_at = []
    updated_at = []
    developer_key_id = []
    user_request = []
    render_time = []
    asset_user_access_id = []
    participated = []
    summarized = []
    http_method = []
    remote_ip = []
    context = []
    asset = []
    app_name = []
    user = []
    user_agent = []
    # loop the lists
    for access in new_data:
        session_id.append(access['session_id'])
        url.append(access['url'])
        context_type.append(access['context_type'])
        asset_type.append(access['asset_type'])
        controller.append(access['controller'])
        action.append(access['action'])
        interaction_seconds.append(access['interaction_seconds'])
        created_at.append(access['created_at'])
        updated_at.append(access['updated_at'])
        developer_key_id.append(access['developer_key_id'])
        user_request.append(access['user_request'])
        render_time.append(access['render_time'])
        asset_user_access_id.append(access['asset_user_access_id'])
        participated.append(access['participated'])
        summarized.append(access['summarized'])
        http_method.append(access['http_method'])
        remote_ip.append(access['remote_ip'])
        user.append(access['links']['user'])
        context.append(access['links']['context'])
        asset.append(access['links']['asset'])
        app_name.append(access['app_name'])
        user_agent.append(access['user_agent'])
    
    # convert to dataframe
    daily = pd.DataFrame({'session_id':session_id,
                          'url':url,
                          'context_type':context_type,
                          'asset_type':asset_type,
                          'controller':controller,
                          'interaction_seconds':interaction_seconds,
                          'created_at':created_at,
                          'updated_at':updated_at,
                          'developer_key_id':developer_key_id,
                          'user_request':user_request,
                          'render_time':render_time,
                          'http_method':http_method,
                          'remote_ip':remote_ip,
                          'summarized':summarized,
                          'context':context,
                          'asset':asset,
                          'app_name':app_name,
                          'user': user,
                          'user_agent': user_agent})

    return daily
