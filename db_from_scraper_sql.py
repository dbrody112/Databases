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
from db_scraper import load_data


criteria_df, sub_criteria_df, final_product_hospital_info = load_data()

@as_declarative()
class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    @declared_attr
    def __table_args__(cls):
        return {'extend_existing':True}


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



Base.metadata.create_all(engine)




final_product_hospital_info["Zip code"][1689]


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
   
    


session.commit()



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



session.commit()

def getCriteria(hospital_name):
    if(len(hospital_name)>= 17 and hospital_name[:17] == "Kaiser Permanente"):
        hospital_name = hospital_name[:19] + " " + hospital_name[19:]
    criteria = session.query(Hospital.hospital_name,Hospital.hrc_score,Criteria.non_discrimination_and_staff_training,Criteria.sub_patient_non_discrimination,                  Criteria.sub_visitation_non_discrimination,Criteria.sub_employment_non_discrimination, Criteria.sub_staff_training,                  Criteria.patient_services_and_support, Criteria.employee_benefits_and_policies, Criteria.sub_employee_policies_and_benefits,                  Criteria.sub_transgender_inclusive_health_insurance, Criteria.patient_and_community_engagement,Criteria.responsible_citizenship).    select_from(Criteria).join(Hospital).    filter(Hospital.hospital_name == hospital_name).all()
    return pd.DataFrame({"Hospital Name":criteria[0][0],"HRC Total Score": criteria[0][1], "Non-Discrimination & Staff Training Total Score":criteria[0][2],"Patient Non-Discrimination Sub-Score":criteria[0][3],                        'Visitation Non-Discrimination Sub-Score':criteria[0][4],'Employment Non-Discrimination Sub-Score':criteria[0][5],'Staff Training Sub-Score':criteria[0][6],'Patient Services & Support Total Score':criteria[0][7],                        'Employee Benefits & Policies Total Score':criteria[0][8],'Employee Policies and Benefits Sub-Score':criteria[0][9],"Transgender Inclusive Health Insurance Sub-Score":criteria[0][10],                        'Patient & Community Engagement Total Score':criteria[0][11],'Responsible Citizenship Total Score':criteria[0][12]},index=[0])
getCriteria('Kaiser Permanente - Manteca Medical Center')


#note that zip code is a string
#also note that since latitude and longitude are not provided to scrape in the website this is the best filtering that can be done.
def filter_by_state_or_zip(state = None, zip_code = None):
    if(state!= None):
        return session.query(Hospital.hospital_name).select_from(Hospital).filter(Hospital.state == state).all()[0][0]
    elif(zip_code!= None):
        return session.query(Hospital.hospital_name).select_from(Hospital).filter(Hospital.zip_code == str(zip_code)).all()[0][0]



filter_by_state_or_zip(zip_code = "93291")


def getAddress(hospital_name):
    if(len(hospital_name)>= 17 and hospital_name[:17] == "Kaiser Permanente"):
        hospital_name = hospital_name[:19] + " " + hospital_name[19:]
    return session.query(Hospital.street_address,Hospital.town,Hospital.state,Hospital.zip_code).select_from(Hospital).filter(Hospital.hospital_name == hospital_name).all()




getAddress(filter_by_state_or_zip(zip_code = "93291"))

