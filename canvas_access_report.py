###############################################################################
#    Access Report with Canvas API
#
#           Ji Guo
#
#           
#
###############################################################################

###############################################################################
# Import packages
###############################################################################


import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import time
import datetime
import os
from zipfile import ZipFile as zip
import json
import multiprocessing
from threading import Thread
import threading
import requests #for take requests
import json #for parse json
import pandas as pd #for dataframe
import time
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil import tz
import random


# a list of tokens

###############################################################################
# API to get the users
###############################################################################


def get_enrollment(canvas_course_id):
    """
    

    Parameters
    ----------
    canvas_course_id : str
        canvas course id.

    Returns
    -------
    final_data : dataframe
        include canvas user id, canvas course id, canvas section id, 
        hawkid, university id, total time, last activity time, and role. 
    """
    start_time = datetime.now()
    print('Pull enrollment data from API for course ', canvas_course_id)
    #randomly pick a token from a list
    token = {"Authorization": """Bearer {}""".format(random.choice(t_list))}
    #setup parameters
    pts = {'per_page': 100,
           'state[]':['active', 'invited', 'creation_pending', 'rejected', 'completed', 'inactive', 'current_and_invited', 'current_and_future', 'current_and_concluded'],
           'include[]':'avatar_url'}
    #get the initial response
    initial_response = requests.get("""https://uiowa.instructure.com/api/v1/courses/{}/enrollments""".format(canvas_course_id), params = pts, headers = token)
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
        if 'https://uiowa.instructure.com/images/messages/avatar-50.png' in access['user']['avatar_url']:
            avatar.append('No')
        else:
            avatar.append('Yes')
            
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
    final_data = final_data[final_data.user_name != 'Test Student']
    print('The process takes', datetime.now() - start_time)
    return final_data



###############################################################################
# selenium
###############################################################################

school = ''
user = ''
password = ''
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
info = webdriver.Chrome(options=options, executable_path=r'c://chromedriver.exe')

info.get("https://{}.instructure.com/".format(school))
time.sleep(3)
hawk_id = info.find_element_by_xpath('/html/body/div/div[2]/div[2]/div[2]/form/div[1]/input')
hawk_id.send_keys(f"{user}")
hawk_id_pass = info.find_element_by_xpath('/html/body/div/div[2]/div[2]/div[2]/form/div[2]/input')
hawk_id_pass.send_keys(f"{password}")
hawk_id_pass.send_keys(Keys.RETURN)
time.sleep(5)

def access_report(canvas_course_id):
    """
    

    Parameters
    ----------
    canvas_course_id : string
        canvas_user_iid.

    Returns
    -------
    data : list
        list of data in JSON.

    """
    data = []
    count = 1
    for i in enrollment.canvas_user_id:
        print(i)
        print(count)
        count = count + 1
        for j in range(1, 100):
            info.get("""https://uiowa.instructure.com/courses/{}/users/{}/usage.json?per_page=100&page={}""".format(canvas_course_id, str(i), str(j)))
            time.sleep(1)
            indi_data = json.loads(info.find_element_by_tag_name('body').text)
            if len(indi_data) == 0:
                break
            else:
                data.append(indi_data)
                
    return data
                



individual_data = []

for i in data:
    for j in i:
        individual_data.append(j['asset_user_access'])

id = []
asset_code = []
asset_group_code = []
user_id = []
context_id = []
context_type = []
last_access = []
created_at = []
updated_at = []
asset_category = []
view_score = []
participate_score = []
action_level = []
display_name = []
membership_type = []
readable_name = []
asset_class_name = []
icon = []

for z in individual_data:
    id.append(z['id'])
    asset_code.append(z['asset_code'])
    asset_group_code.append(z['asset_group_code'])
    user_id.append(z['user_id'])
    context_id.append(z['context_type'])
    context_type.append(z['context_type'])
    last_access.append(z['last_access'])
    created_at.append(z['created_at'])
    updated_at.append(z['updated_at'])
    asset_category.append(z['asset_category'])
    view_score.append(z['view_score'])
    participate_score.append(z['participate_score'])
    action_level.append(z['action_level'])
    display_name.append(z['display_name'])
    membership_type.append(z['membership_type'])
    readable_name.append(z['readable_name'])
    asset_class_name.append(z['asset_class_name'])
    icon.append(z['icon'])


final = pd.DataFrame({'id':id,
                      'asset_code':asset_code,
                      'asset_group_code':asset_group_code,
                      'user_id':user_id,
                      'context_id':context_id,
                      'context_type':context_type,
                      'context_type':context_type,
                      'last_access':last_access,
                      'created_at':created_at,
                      'updated_at':updated_at,
                      'asset_category':asset_category,
                      'view_score':view_score,
                      'participate_score':participate_score,
                      'action_level':action_level,
                      'display_name':display_name,
                      'membership_type':membership_type,
                      'readable_name':readable_name,
                      'asset_class_name':asset_class_name,
                      'icon':icon})

final_data = pd.merge(final, enrollment,
                      left_on = 'user_id',
                      right_on = 'canvas_user_id',
                      how = 'outer')

final_data.columns

data_final = final_data[['canvas_course_id',
                         'canvas_section_id',
                         'canvas_user_id',
                         'hawk_id',
                         'university_id',
                         'user_name',
                         'status',
                         'role',
                         'total_activity_time',
                         'avatar',
                         'asset_code',
                         'last_access',
                         'created_at',
                         'updated_at',
                         'asset_category',
                         'view_score',
                         'participate_score',
                         'readable_name',
                         'asset_class_name']]

data_final.columns = ['canvas_course_id', 'canvas_section_id', 'canvas_user_id', 'hawk_id',
       'university_id', 'user_name', 'status', 'role', 'total_activity_time',
       'avatar', 'access_identifier', 'last_access', 'first_access',
       'updated_time', 'access_category', 'viewed_times', 'participate_times', 'access_name', 'type']



def user_time_conversion(dataframe):
    df = dataframe.copy()
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    for i in range(len(df)):
        print(i)
        ul = df.last_access[i]
        uf = df.first_access[i]
        uu = df.updated_time[i]
        if type(ul) == float:
            continue
        else:
            utc_ul= datetime.strptime(ul, '%Y-%m-%dT%H:%M:%SZ')
            utc_uf= datetime.strptime(uf, '%Y-%m-%dT%H:%M:%SZ')
            utc_uu= datetime.strptime(uu, '%Y-%m-%dT%H:%M:%SZ')
            c_ul = utc_ul.replace(tzinfo=from_zone).astimezone(to_zone)
            c_uf = utc_uf.replace(tzinfo=from_zone).astimezone(to_zone)
            c_uu = utc_uu.replace(tzinfo=from_zone).astimezone(to_zone)
            df.last_access[i] = c_ul.replace(tzinfo=None)
            df.first_access[i] = c_uf.replace(tzinfo=None)
            df.updated_time[i] = c_uu.replace(tzinfo=None)
    return df


access_report = user_time_conversion(data_final)


