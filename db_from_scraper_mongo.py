import pymongo
import dns
from pymongo import MongoClient
import pprint
from db_scraper import load_data

criteria_df, sub_criteria_df, final_product_hospital_info = load_data()

client = MongoClient('mongodb+srv://<USERNAME>:<PASSWORD>@<DATABASE>.mongodb.net/<TABLE>')
db_hospital = client.hospital
db_criteria = client.criteria
for i in range(len(final_product_hospital_info)):
    db_hospital.hospital.insert_one({"Title":final_product_hospital_info["Title"][i],"Google Maps URL":final_product_hospital_info["Google Maps URL"][i],"Phone Number":final_product_hospital_info["Phone Number"][i],"Website":final_product_hospital_info["Website"][i],"Street":final_product_hospital_info["Street"][i],"Town":final_product_hospital_info["Town"][i],"State":final_product_hospital_info["State"][i],"Zip code":final_product_hospital_info["Zip code"][i],"Score":final_product_hospital_info["Score"][i],"Score Breakdown":[{'Non-Discrimination & Staff Training':criteria_df['Non-Discrimination & Staff Training'][i], 'Non-Discrimination & Staff Training Breakdown':[{'Patient Non-Discrimination':sub_criteria_df['Patient Non-Discrimination'][i],'Visitation Non-Discrimination':sub_criteria_df['Visitation Non-Discrimination'][i],'Employment Non-Discrimination':sub_criteria_df['Employment Non-Discrimination'][i],'Staff Training':sub_criteria_df['Staff Training'][i]}],'Patient Services & Support':criteria_df['Patient Services & Support'][i],'Employee Benefits & Policies':criteria_df['Employee Benefits & Policies'][i], "Employee Benefits & Policies Breakdown":[{'Employee Policies and Benefits':sub_criteria_df['Employee Policies and Benefits'][i],'Transgender Inclusive Health Insurance':sub_criteria_df['Transgender Inclusive Health Insurance'][i]}],'Patient & Community Engagement':criteria_df['Patient & Community Engagement'][i],'Responsible Citizenship':criteria_df['Responsible Citizenship'][i]}
    ]})
for obj in db_hospital.hospital.find({"Score": {"$exists": True}}):
    if(obj['Score'] != 'N/A'):
        db_hospital.hospital.update_one({"_id":obj["_id"]},{"$set":{"Score":int(obj["Score"])}})
    else:
        db_hospital.hospital.update_one({"_id":obj["_id"]},{"$set":{"Score":-1}})      
        
#state must be abbreviated
#-1 is the N/A for the score column
def rank_hospitals_in_zip_or_state(zip_code = None, state = None):
    hospital_array = []
    if(zip_code!= None):
        for i in db_hospital.hospital.find({"Zip code":str(zip_code)},{"Title":1,"Score":1}).sort([("Score",-1)]):
            print("Title: " + i['Title'] +"\n"+ "Score : " + str(i["Score"]))
            print(" ")
            hospital_array.append([i["Title"],i["Score"],i["_id"]])
    elif(state != None):
        for i in db_hospital.hospital.find({"State":str(state)},{"Title":1,"Score":1}).sort([("Score",-1)]):
            print("Title: " + i['Title'] +"\n"+ "Score : " + str(i["Score"]))
            print(" ")
            hospital_array.append([i["Title"],i["Score"],i["_id"]])
    return pd.DataFrame({"Hospital Name":np.array(hospital_array)[:,0],"HRC Score":np.array(hospital_array)[:,1],"Hospital ID":np.array(hospital_array)[:,2]})
hospital_df = rank_hospitals_in_zip_or_state(state = "NJ")
def getAddress(hospital_name=None,df_option = []):
    address_array = []
    if(hospital_name != None):
        if(len(hospital_name)>= 17 and hospital_name[:17] == "Kaiser Permanente"):
            hospital_name = hospital_name[:19] + " " + hospital_name[19:]
        for i in db_hospital.hospital.find({"Title":{"$exists":True}, "Title":hospital_name,"_id":j["_id"]},{"Title":1,"Street":1,"Town":1,"State":1, "Zip code": 1, "_id":0}):
            pprint.pprint(i)
        
    if(len(df_option)!= 0):
        for j in range(len(df_option)):
            for i in db_hospital.hospital.find({"Title":{"$exists":True}, "Title":df_option["Hospital Name"][j],"_id":df_option["Hospital ID"][j]},{"Title":1,"Street":1,"Town":1,"State":1, "Zip code": 1, "_id":0}):
                address_array.append([i["Title"],i["Street"],i["Town"],i["State"],i["Zip code"]])
    address_numpy = np.array(address_array)
    address_df = pd.DataFrame({"Hospital Name":address_numpy[:,0],"Street":address_numpy[:,1],"Town":address_numpy[:,2],"State":address_numpy[:,3],"Zip code":address_numpy[:,4]})
    return address_df.merge(df_option, left_on = "Hospital Name",right_on = "Hospital Name")
def getScoreBreakdown(hospital_name,hospital_id):
    if(len(hospital_name)>= 17 and hospital_name[:17] == "Kaiser Permanente"):
        hospital_name = hospital_name[:19] + " " + hospital_name[19:]
    for i in db_hospital.hospital.find({"Score Breakdown":{"$exists":True}, "Title":hospital_name, "_id":hospital_id},{"Title":1,"Score":1,"Score Breakdown":1,"_id":0}).sort([("Score",-1)]):
        pprint.pprint(i)
getAddress(df_option = hospital_df)
for i in range(len(hospital_df)):
    getScoreBreakdown(hospital_df["Hospital Name"][i], hospital_df["Hospital ID"][i])
