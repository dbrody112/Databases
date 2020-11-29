


#create a new sqlite database
#create database fixture
#here I show 2 of the 3 solutions I suggested: employee solution and advertising solution
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///sailors.db',echo = True)


def campaignPerformanceDay(itype="all",inventory_date = None):
    if(itype == "one"):
        return session.query(func.sum(Boat.price_per_hour * (Reservation.time_out - Reservation.time_in)),Reservation.day,Campaign.advertisements,Campaign.sales).    select_from(Reservation).join(Boat).join(Campaign, Reservation.day == Campaign.date).filter(Reservation.day == inventory_date).all() 
    if(itype == "all"):
        return session.query(func.sum(Boat.price_per_hour * (Reservation.time_out - Reservation.time_in)),Reservation.day,Campaign.advertisements,Campaign.sales).    select_from(Reservation).join(Boat).join(Campaign, Reservation.day == Campaign.date).group_by(Reservation.day).all() 



import datetime as dt

#assumes no nonetype values
def campaignPerformanceOverTime(days=0,itype="all",inventory_date = None):

    if(itype == "one"):
        x = []
        for i in range(days):
            x.append(session.query(func.sum(Boat.price_per_hour*(Reservation.time_out - Reservation.time_in)),Reservation.day).                     select_from(Reservation).join(Boat).filter(Reservation.day == inventory_date-dt.timedelta(i+1)).all())
        sum_before = 0
        for i in range(days):
            sum_before+=x[i][0][0]
        x = []
        for i in range(days):
            x.append(session.query(func.sum(Boat.price_per_hour*(Reservation.time_out - Reservation.time_in)),Reservation.day).                     select_from(Reservation).join(Boat).filter(Reservation.day == inventory_date+dt.timedelta(i+1)).all())
        sum_after = 0
        for i in range(days):
            sum_after+=x[i][0][0]
        return f"the revenue before is {sum_before} and the revenue after is {sum_after}"
    if(itype == "all"):
        g = []
        h =[]
        for i in range(len(session.query(Campaign.date).select_from(Campaign).all())):
            g.append(session.query(Campaign.date).select_from(Campaign).all()[i][0])
        for j in g:
            x= []
            for i in range(days):
                x.append(session.query(func.sum(Boat.price_per_hour*(Reservation.time_out - Reservation.time_in)),Reservation.day).                         select_from(Reservation).join(Boat).filter(Reservation.day == j-dt.timedelta(i+1)).all())
            sum_before = 0
            for i in range(days):
                sum_before+=x[i][0][0]
            x=[]
            for i in range(days):
                x.append(session.query(func.sum(Boat.price_per_hour*(Reservation.time_out - Reservation.time_in)),Reservation.day).                         select_from(Reservation).join(Boat).filter(Reservation.day == j+dt.timedelta(i+1)).all())
            sum_after = 0
            for i in range(days):
                sum_after+=x[i][0][0]
            h.append([sum_before,sum_after,j,days])
            print(f"there was {sum_before} {days} days before {j} and {sum_after} {days} days after {j}")
            
        return h
    
 
campaignPerformanceOverTime(days = 1, itype = "one", inventory_date = date(1998,10,10))


campaignPerformanceDay()


def latesDesc():
    return session.query(func.count(Schedule.schedule_id),Schedule.name).    select_from(Schedule).    filter(Schedule.signed_out-Schedule.signed_in < (17-9)).group_by(Schedule.name).    order_by(func.count(Schedule.schedule_id).desc()).all()


def salesAsc():
    return session.query(func.sum(Boat.price_per_hour * (Reservation.time_out - Reservation.time_in)),Schedule.name).    select_from(Reservation).join(Boat).join(Schedule).    group_by(Schedule.name).order_by(func.sum(Boat.price_per_hour * (Reservation.time_out - Reservation.time_in))).all()

def firingProcess(priority = 'lates'):
    if(priority == 'lates'):
        return (latesDesc()[0][1])
    if(priority == 'sales'):
        return (salesAsc()[0][1])    


firingProcess()



def boat_availability(boat,date):
    if(len(session.query(Boat).join(Reservation).filter(and_(Boat.bname == boat,Reservation.time_out.isnot(None),Reservation.day == date)).all()) > 0:
       return True
    else:
       return False

