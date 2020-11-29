#!/usr/bin/env python
# coding: utf-8

# In[1]:


from scraper_utils import scrape
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import sqlalchemy
import regex as re


# In[2]:


def load_data(csv_path = "nst-est2019-01.csv")
    state_csv = pd.read_csv(csv_path)
    df = pd.DataFrame({'state':np.array(state_csv)[:,0][8:59].astype(str)}).applymap(lambda x: x.replace(".","").strip())
    hospital_info=[]
    sub_criteria_info = []
    criteria_info = []
    #scrape(hospital_info = hospital_info,sub_criteria_info = sub_criteria_info)
    for i in df['state']:
        scrape(string_input = i, hospital_info = hospital_info,sub_criteria_info= sub_criteria_info,criteria_info = criteria_info)


    split_scrape = []
    for j in range(len(np.array(hospital_info)[1])):
        col = []
        for i in range(len(np.array(hospital_info))):
            if(i%1600==0 and i!=0):
                print(j)
            if(len(np.array(hospital_info)[i]) == 8):
                (np.array(hospital_info)[i]).insert(2,"N/A")
            col.append((np.array(hospital_info))[i][j])
        split_scrape.append(col)
 
    final_product_hospital_info = pd.DataFrame({"Title":split_scrape[0],"Google Maps URL":split_scrape[1],"Phone Number":split_scrape[2],"Website":split_scrape[3],"Street":split_scrape[4],"Town":split_scrape[5],"State":split_scrape[6],"Zip code":split_scrape[7],"Score":split_scrape[8]})


    final_product_hospital_info['Street']


    final_product_hospital_info['Phone Number'] = final_product_hospital_info['Phone Number'].apply(lambda x: x[4:].strip())


    import math
    for i in range(len(final_product_hospital_info['Zip code'])):
        try:
            b = int(final_product_hospital_info['Zip code'][i])
        except:
            try:
                final_product_hospital_info['Zip code'][i] = (final_product_hospital_info["Google Maps URL"][i][-10:])
                int(final_product_hospital_info["Google Maps URL"][i][-10:].split("-")[0])
            except:
                final_product_hospital_info['Zip code'][i] = (final_product_hospital_info["Google Maps URL"][i][-5:])
                try:
                    int(final_product_hospital_info["Google Maps URL"][i][-5:])
                except:
                    final_product_hospital_info['Zip code'][i] = "N/A"

    final_product_hospital_info['Delivery Code'] = final_product_hospital_info['Zip code'].apply(lambda x: np.array(x.split("-"))[1:2])
    final_product_hospital_info['Zip code'] = final_product_hospital_info['Zip code'].apply(lambda x: (np.array(x.split("-"))[:1][0]))


    final_product_hospital_info['Delivery Code'] = (final_product_hospital_info['Delivery Code']).apply(lambda x: x[0] if x.size>0 else math.nan)

    final_product_hospital_info['Street'] = final_product_hospital_info['Street'].apply(str)


    final_product_hospital_info['Street']



    str(final_product_hospital_info['Street'][1655])


    import googlemaps
    gmaps = googlemaps.Client('AIzaSyArVIqU7hvCytzs1IcF7r4-It5jaiDFla0')
    gmaps.geocode('')


    import googlemaps
    gmaps = googlemaps.Client(<API KEY>)
    lat = []
    long = []
    for i in range(len(final_product_hospital_info)):
        try:
            geocode_result = gmaps.geocode(final_product_hospital_info["Street"][i] + ", " + final_product_hospital_info["Town"][i] + ", " + final_product_hospital_info['State'][i])
            latitude = np.array(geocode_result)[0]['geometry']['location']['lat']
            longitude = np.array(geocode_result)[0]['geometry']['location']['lng']
            lat.append(latitude)
            long.append(longitude)
        except:
            latitude = -1
            longitude = -1
            lat.append(latitude)
            long.append(longitude)

    final_product_hospital_info['latitude'] = lat
    final_product_hospital_info['longitude'] = long
    final_product_hospital_info['longitude']

    final_product_hospital_info[final_product_hospital_info['latitude']==-1]

    final_product_hospital_info['latitude'][0] = 33.5066
    final_product_hospital_info['longitude'][0] = -86.8031

    manual_indexes = []
    final_product_hospital_info["Street"][412] = '800 W Central Rd'
    final_product_hospital_info["Town"][412] = 'Arlington Heights'
    final_product_hospital_info["State"][412] = 'IL'
    manual_indexes.append(412)

    final_product_hospital_info["Street"][458] = '1221 S Gear Ave'
    final_product_hospital_info["Town"][458] = 'West Burlington'
    final_product_hospital_info["State"][458] = 'IA'
    manual_indexes.append(458)

    final_product_hospital_info["Street"][496] = '2800 Clay Edwards Dr'
    final_product_hospital_info["Town"][496] = 'North Kansas City'
    final_product_hospital_info["State"][496] = 'MO'
    manual_indexes.append(496)

    final_product_hospital_info["Street"][682] = '2316 S Cedar St'
    final_product_hospital_info["Town"][682] = 'Lansing'
    final_product_hospital_info["State"][682] = 'MI'
    manual_indexes.append(682)

    final_product_hospital_info["Street"][697] = '27351 Dequindre Rd'
    final_product_hospital_info["Town"][697] = 'Madison Heights'
    final_product_hospital_info["State"][697] = 'MI'
    manual_indexes.append(697)

    final_product_hospital_info["Street"][920] = '2 Stone Harbor Blvd'
    final_product_hospital_info["Town"][920] = 'Cape May Court House'
    final_product_hospital_info["State"][920] = 'NJ'
    manual_indexes.append(920)

    final_product_hospital_info["Street"][1151] = '27100 Chardon Rd'
    final_product_hospital_info["Town"][1151] = 'Richmond Heights'
    final_product_hospital_info["State"][1151] = 'OH'
    manual_indexes.append(1151)

    for i in manual_indexes:
        geocode_result = gmaps.geocode(final_product_hospital_info["Street"][i] + ", " + final_product_hospital_info["Town"][i] + ", " + final_product_hospital_info['State'][i])
        latitude = np.array(geocode_result)[0]['geometry']['location']['lat']
        longitude = np.array(geocode_result)[0]['geometry']['location']['lng']
        final_product_hospital_info['latitude'][i] = latitude
        final_product_hospital_info['longitude'][i] = longitude
    

    for i in sub_criteria_info:
        if(len(i)==3):
            i.append('N/A')
            i.append('N/A')
            i.append('N/A')
        

    split_scrape = []
    for j in range(len(np.array(sub_criteria_info)[1])):
        col = []
        for i in range(len(np.array(sub_criteria_info))):
            if(i%1600==0 and i!=0):
                print(j)
            col.append((np.array(sub_criteria_info))[i][j])
        split_scrape.append(col)
    
    sub_criteria_df = pd.DataFrame({'Patient Non-Discrimination':split_scrape[0],'Visitation Non-Discrimination':split_scrape[1],'Employment Non-Discrimination':split_scrape[2],'Staff Training':split_scrape[3],'Employee Policies and Benefits':split_scrape[4],'Transgender Inclusive Health Insurance':split_scrape[5]})

    split_scrape = []
    for j in range(len(np.array(criteria_info)[1])):
        col = []
        for i in range(len(np.array(criteria_info))):
            if(i%1600==0 and i!=0):
                print(j)
            col.append((np.array(criteria_info))[i][j])
        split_scrape.append(col)

    criteria_df = pd.DataFrame({'Non-Discrimination & Staff Training':split_scrape[0],'Patient Services & Support':split_scrape[1],'Employee Benefits & Policies':split_scrape[2],'Patient & Community Engagement':split_scrape[3],'Responsible Citizenship':split_scrape[4]})


    final_product_hospital_info.to_csv(r'<PATH>\final_product_hospital_info.csv')
    sub_criteria_df.to_csv(r'<PATH>\sub_criteria_df.csv')
    criteria_df.to_csv(r'<PATH>\criteria_df.csv')

    return criteria_df, sub_criteria_df, final_product_hospital_info
