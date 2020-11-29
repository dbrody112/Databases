#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ipynb.fs.full.databases_assignment_2_scraper_utils import scrape
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import sqlalchemy
import regex as re


# In[2]:



state_csv = pd.read_csv("nst-est2019-01.csv")
df = pd.DataFrame({'state':np.array(state_csv)[:,0][8:59].astype(str)}).applymap(lambda x: x.replace(".","").strip())
hospital_info=[]
sub_criteria_info = []
criteria_info = []
#scrape(hospital_info = hospital_info,sub_criteria_info = sub_criteria_info)
for i in df['state']:
    scrape(string_input = i, hospital_info = hospital_info,sub_criteria_info= sub_criteria_info,criteria_info = criteria_info)
    


# In[3]:


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
    


# In[4]:


final_product_hospital_info = pd.DataFrame({"Title":split_scrape[0],"Google Maps URL":split_scrape[1],"Phone Number":split_scrape[2],"Website":split_scrape[3],"Street":split_scrape[4],"Town":split_scrape[5],"State":split_scrape[6],"Zip code":split_scrape[7],"Score":split_scrape[8]})


# In[5]:


final_product_hospital_info['Street']


# In[6]:


final_product_hospital_info['Phone Number'] = final_product_hospital_info['Phone Number'].apply(lambda x: x[4:].strip())


# In[ ]:





# In[7]:


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


# In[8]:


final_product_hospital_info['Delivery Code'] = final_product_hospital_info['Zip code'].apply(lambda x: np.array(x.split("-"))[1:2])
final_product_hospital_info['Zip code'] = final_product_hospital_info['Zip code'].apply(lambda x: (np.array(x.split("-"))[:1][0]))


# In[9]:


final_product_hospital_info['Delivery Code'] = (final_product_hospital_info['Delivery Code']).apply(lambda x: x[0] if x.size>0 else math.nan)


# In[10]:


final_product_hospital_info['Street'] = final_product_hospital_info['Street'].apply(str)


# In[11]:


final_product_hospital_info['Street']


# In[12]:


re.search('  <br/> ',str(final_product_hospital_info['Street'][1655])).start()


# In[13]:


str(final_product_hospital_info['Street'][1655])


# In[9]:


import googlemaps
gmaps = googlemaps.Client('AIzaSyArVIqU7hvCytzs1IcF7r4-It5jaiDFla0')
gmaps.geocode('')


# In[14]:


import googlemaps
gmaps = googlemaps.Client('AIzaSyArVIqU7hvCytzs1IcF7r4-It5jaiDFla0')
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


# In[15]:


final_product_hospital_info['latitude'] = lat
final_product_hospital_info['longitude'] = long


# In[10]:


final_product_hospital_info['longitude']


# In[16]:


final_product_hospital_info[final_product_hospital_info['latitude']==-1]


# In[17]:


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
    


# In[18]:


for i in sub_criteria_info:
    if(len(i)==3):
        i.append('N/A')
        i.append('N/A')
        i.append('N/A')
        


# In[19]:


split_scrape = []
for j in range(len(np.array(sub_criteria_info)[1])):
    col = []
    for i in range(len(np.array(sub_criteria_info))):
        if(i%1600==0 and i!=0):
            print(j)
        col.append((np.array(sub_criteria_info))[i][j])
    split_scrape.append(col)
    


# In[20]:


sub_criteria_df = pd.DataFrame({'Patient Non-Discrimination':split_scrape[0],'Visitation Non-Discrimination':split_scrape[1],'Employment Non-Discrimination':split_scrape[2],'Staff Training':split_scrape[3],'Employee Policies and Benefits':split_scrape[4],'Transgender Inclusive Health Insurance':split_scrape[5]})


# In[21]:


split_scrape = []
for j in range(len(np.array(criteria_info)[1])):
    col = []
    for i in range(len(np.array(criteria_info))):
        if(i%1600==0 and i!=0):
            print(j)
        col.append((np.array(criteria_info))[i][j])
    split_scrape.append(col)


# In[22]:


criteria_df = pd.DataFrame({'Non-Discrimination & Staff Training':split_scrape[0],'Patient Services & Support':split_scrape[1],'Employee Benefits & Policies':split_scrape[2],'Patient & Community Engagement':split_scrape[3],'Responsible Citizenship':split_scrape[4]})


# In[23]:


final_product_hospital_info.to_csv(r'C:\Users\potat\react-flask-app\api\final_product_hospital_info.csv')
sub_criteria_df.to_csv(r'C:\Users\potat\react-flask-app\api\sub_criteria_df.csv')
criteria_df.to_csv(r'C:\Users\potat\react-flask-app\api\criteria_df.csv')


# In[24]:


final_product_hospital_info['Zip code']


# In[25]:


'''CREATE TABLE Hospitals(hospital_id INT NOT NULL
zip_code CHAR(5),
longitude FLOAT NOT NULL,
latitude FLOAT NOT NULL,
phone_number VARCHAR(20),
hospital_name VARCHAR(50) NOT NULL,
website VARCHAR(200),
hrc_score INT CHECK(hrc_score BETWEEN 0 AND 100),
street_address VARCHAR(50),
state VARCHAR(20),
town VARCHAR(30),
PRIMARY KEY(hospital_id));'''


# In[ ]:





# In[26]:


from sqlalchemy import Column, Integer, String,Date,Float,Numeric
from sqlalchemy import Table, MetaData
meta = MetaData()
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.declarative.api import declared_attr
from datetime import date
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
engine = create_engine("mysql://root:new_password@localhost/hospitals")
Session = sessionmaker(bind = engine)
session = Session()


@as_declarative()
class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    @declared_attr
    def __table_args__(cls):
        return {'extend_existing':True}


# In[27]:


final_product_hospital_info["Score"][1680]


# In[28]:


from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKey

class Hospital(Base):
    
    __tablename__ = 'hospital'
    __table_args__ = (
            CheckConstraint('hospital_name IS NOT NULL'),
            CheckConstraint('hrc_score BETWEEN 0 AND 100'),
            )
    
    zip_code = Column(String(length = 5))
    #to be implemented using google maps api
    latitude = Column(Float)
    longitude = Column(Float)
    phone_number = Column(String(length = 20))
    hospital_name = Column(String(length=100))
    website = Column(String(length = 200))
    mapsUrl = Column(String(length=200))
    hrc_score = Column(Integer)
    street_address = Column(String(length = 200))
    state = Column(String(length = 10))
    town = Column(String(length = 100))
    hospital_id = Column(Integer,primary_key = True)
    
    
    
class Criteria(Base):
    
    __tablename__ = 'criteria'
    
    #* for now these columns are strings however in the future they may become integers with check constraints.
    # for the purpose of this assignment they will stay as strings
    #sub stands for sub criteria btw
    
    non_discrimination_and_staff_training = Column(String(length = 10))
    sub_patient_non_discrimination = Column(String(length = 10))
    sub_visitation_non_discrimination = Column(String(length = 10))
    sub_employment_non_discrimination = Column(String(length = 10))
    sub_staff_training = Column(String(length = 10))
    patient_services_and_support = Column(String(length = 10))
    employee_benefits_and_policies = Column(String(length = 10))
    sub_employee_policies_and_benefits = Column(String(length = 10))
    sub_transgender_inclusive_health_insurance = Column(String(length = 10))
    patient_and_community_engagement = Column(String(length = 10))
    responsible_citizenship = Column(String(length = 100))
    hospital_id = Column(Integer,ForeignKey('hospital.hospital_id'),primary_key = True)


# In[29]:


Base.metadata.create_all(engine)


# In[30]:


final_product_hospital_info["Zip code"][1689]


# In[31]:


#{"Title":split_scrape[0],"Google Maps URL":split_scrape[1],"Phone Number":split_scrape[2],"Website":split_scrape[3],"Street":split_scrape[4],"Town":split_scrape[5],"State":split_scrape[6],"Zip code":split_scrape[7],"Score":split_scrape[8]})
j = 1
for i in range(len(final_product_hospital_info)):
    
    zip_code = final_product_hospital_info["Zip code"][i]
    latitude = final_product_hospital_info["latitude"][i]
    longitude = final_product_hospital_info["longitude"][i]
    phone_number = final_product_hospital_info["Phone Number"][i]
    hospital_name = final_product_hospital_info["Title"][i]
    website = final_product_hospital_info["Website"][i]
    mapsUrl = final_product_hospital_info["Google Maps URL"][i]
    hrc_score = final_product_hospital_info["Score"][i]
    street_address = final_product_hospital_info["Street"][i]
    state = final_product_hospital_info["State"][i]
    town = final_product_hospital_info["Town"][i]
    
    if(hrc_score == "N/A"):
        session.add(Hospital(zip_code = zip_code,latitude = latitude, longitude = longitude, phone_number = phone_number, hospital_name = hospital_name, website = website, mapsUrl = mapsUrl,                        street_address = street_address,state = state, town = town, hospital_id = j))
    else:
         session.add(Hospital(zip_code = zip_code,latitude = latitude, longitude = longitude,phone_number = phone_number, hospital_name = hospital_name, website = website, mapsUrl = mapsUrl,                        hrc_score = int(hrc_score),street_address = street_address,state = state, town = town, hospital_id = j))
        
            
    j+=1
   
    
    


# In[32]:


session.commit()


# In[33]:


j=1
for i in range(len(sub_criteria_df)):
    
    non_discrimination_and_staff_training = criteria_df['Non-Discrimination & Staff Training'][i]
    sub_patient_non_discrimination = sub_criteria_df['Patient Non-Discrimination'][i]
    sub_visitation_non_discrimination = sub_criteria_df['Visitation Non-Discrimination'][i]
    sub_employment_non_discrimination = sub_criteria_df['Employment Non-Discrimination'][i]
    sub_staff_training = sub_criteria_df['Staff Training'][i]
    
    patient_services_and_support = criteria_df['Patient Services & Support'][i]
    
    employee_benefits_and_policies = criteria_df['Employee Benefits & Policies'][i]
    sub_employee_policies_and_benefits = sub_criteria_df['Employee Policies and Benefits'][i]
    sub_transgender_inclusive_health_insurance = sub_criteria_df['Transgender Inclusive Health Insurance'][i]
    
    patient_and_community_engagement = criteria_df['Patient & Community Engagement'][i]
    
    responsible_citizenship = criteria_df['Responsible Citizenship'][i]


    session.add(Criteria(non_discrimination_and_staff_training = non_discrimination_and_staff_training, sub_patient_non_discrimination = sub_patient_non_discrimination,                          sub_visitation_non_discrimination = sub_visitation_non_discrimination, sub_employment_non_discrimination = sub_employment_non_discrimination,                         sub_staff_training = sub_staff_training,patient_services_and_support=patient_services_and_support,employee_benefits_and_policies=employee_benefits_and_policies,                         sub_employee_policies_and_benefits = sub_employee_policies_and_benefits, sub_transgender_inclusive_health_insurance=sub_transgender_inclusive_health_insurance,                         patient_and_community_engagement=patient_and_community_engagement,responsible_citizenship=responsible_citizenship,hospital_id = j))
    j+=1


# In[34]:


session.commit()


# In[35]:



def getCriteria(hospital_name):
    if(len(hospital_name)>= 17 and hospital_name[:17] == "Kaiser Permanente"):
        hospital_name = hospital_name[:19] + " " + hospital_name[19:]
    criteria = session.query(Hospital.hospital_name,Hospital.hrc_score,Criteria.non_discrimination_and_staff_training,Criteria.sub_patient_non_discrimination,                  Criteria.sub_visitation_non_discrimination,Criteria.sub_employment_non_discrimination, Criteria.sub_staff_training,                  Criteria.patient_services_and_support, Criteria.employee_benefits_and_policies, Criteria.sub_employee_policies_and_benefits,                  Criteria.sub_transgender_inclusive_health_insurance, Criteria.patient_and_community_engagement,Criteria.responsible_citizenship).    select_from(Criteria).join(Hospital).    filter(Hospital.hospital_name == hospital_name).all()
    return pd.DataFrame({"Hospital Name":criteria[0][0],"HRC Total Score": criteria[0][1], "Non-Discrimination & Staff Training Total Score":criteria[0][2],"Patient Non-Discrimination Sub-Score":criteria[0][3],                        'Visitation Non-Discrimination Sub-Score':criteria[0][4],'Employment Non-Discrimination Sub-Score':criteria[0][5],'Staff Training Sub-Score':criteria[0][6],'Patient Services & Support Total Score':criteria[0][7],                        'Employee Benefits & Policies Total Score':criteria[0][8],'Employee Policies and Benefits Sub-Score':criteria[0][9],"Transgender Inclusive Health Insurance Sub-Score":criteria[0][10],                        'Patient & Community Engagement Total Score':criteria[0][11],'Responsible Citizenship Total Score':criteria[0][12]},index=[0])


# In[36]:


getCriteria('Kaiser Permanente - Manteca Medical Center')


# In[ ]:





# In[328]:


#note that zip code is a string
#also note that since latitude and longitude are not provided to scrape in the website this is the best filtering that can be done.
def filter_by_state_or_zip(state = None, zip_code = None):
    if(state!= None):
        return session.query(Hospital.hospital_name).select_from(Hospital).filter(Hospital.state == state).all()[0][0]
    elif(zip_code!= None):
        return session.query(Hospital.hospital_name).select_from(Hospital).filter(Hospital.zip_code == str(zip_code)).all()[0][0]


# In[329]:


filter_by_state_or_zip(zip_code = "93291")


# In[332]:


def getAddress(hospital_name):
    if(len(hospital_name)>= 17 and hospital_name[:17] == "Kaiser Permanente"):
        hospital_name = hospital_name[:19] + " " + hospital_name[19:]
    return session.query(Hospital.street_address,Hospital.town,Hospital.state,Hospital.zip_code).select_from(Hospital).filter(Hospital.hospital_name == hospital_name).all()


# In[333]:


getAddress(filter_by_state_or_zip(zip_code = "93291"))


# In[ ]:





# In[ ]:




