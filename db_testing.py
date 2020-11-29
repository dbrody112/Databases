
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///sailors.db',echo = True)

Session = sessionmaker(bind = engine)
session = Session()

sailors = [
	(22,"dusting",7,45.0),
	(23,"emilio",7,45.0),
	(24,"scruntus",1,33.0),
	(29,"brutus",1,33.0),
	(31,"lubber",8,55.5),
	(32,"andy",8,25.5),
	(35,"figaro",8,55.5),
	(58,"rusty",10,35),
	(59,"stum",8,25.5),
	(60,"jit",10,35),
	(61,"ossola",7,16),
	(62,"shaun",10,35),
	(64,"horatio",7,16),
	(71,"zorba",10,35),
	(74,"horatio",9,25.5),
	(85,"art",3,25.5),
	(88,"kevin",3,25.5),
	(89,"will",3,25.5),
	(90,"josh",3,25.5),
	(95,"bob",3,63.5),
]

boats = [
	(101,'Interlake','blue',45),
	(102,'Interlake','red',45),
	(103,'Clipper','green',40),
	(104,'Clipper','red',40),
	(105,'Marine','red',35),
	(106,'Marine','green',35),
	(107,'Marine','blue',35),
	(108,'Driftwood','red',35),
	(109,'Driftwood','blue',35),
	(110,'Klapser','red',30),
	(111,'Sooney','green',28),
	(112,'Sooney','red',28),
]

reserves = [
	(22,101,"1998-10-10"),
	(22,102,"1998-10-10"),
	(22,103,"1998-08-10"),
	(22,104,"1998-07-10"),
	(23,104,"1998-10-10"),
	(23,105,"1998-11-10"),
	(24,104,"1998-10-10"),
	(31,102,"1998-11-10"),
	(31,103,"1998-11-06"),
	(31,104,"1998-11-12"),
	(35,104,"1998-08-10"),
	(35,105,"1998-11-06"),
	(59,105,"1998-07-10"),
	(59,106,"1998-11-12"),
	(59,109,"1998-11-10"),
	(60,106,"1998-09-05"),
	(60,106,"1998-09-08"),
	(60,109,"1998-07-10"),
	(61,112,"1998-09-08"),
	(62,110,"1998-11-06"),
	(64,101,"1998-09-05"),
	(64,102,"1998-09-08"),
	(74,103,"1998-09-08"),
	(88,107,"1998-09-08"),
	(88,110,"1998-09-05"),
	(88,110,"1998-11-12"),
	(88,111,"1998-09-08"),
	(89,108,"1998-10-10"),
	(89,109,"1998-08-10"),
	(90,109,"1998-10-10"),
]

from sqlalchemy import Column, Integer, String,Date,Float
from sqlalchemy import Table, MetaData
meta = MetaData()
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.declarative.api import declared_attr
from datetime import date

@as_declarative()
class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    @declared_attr
    def __table_args__(cls):
        return {'extend_existing':True}

#implement create table
dates = []
for (i,b,v) in reserves:
    dates.append(date(int(v[:4]), int(v[5:7]), int(v[8:10])))


# In[2]:


from sqlalchemy import ForeignKey
class Sailor(Base):
    
    __tablename__ = 'Sailors'
    
    sid = Column(Integer, primary_key=True)
    sname = Column(String)
    rating = Column(Integer)
    age = Column(Integer)
    
    def __repr__(self):
        return "<User(id='%d', name='%s', rating='%d', age = %s)>" % (
                             self.sid, self.sname, self.rating,self.age)

class Reservation(Base):
    
    __tablename__ = 'Reserves'
    
    sid = Column(Integer,ForeignKey('Sailors.sid'),primary_key=True)
    bid = Column(Integer,ForeignKey('Boats.bid'), primary_key=True)
    day = Column(Date, primary_key=True)
    
    def __repr__(self):
        return "<User(boat id='%d', sailor id ='%d', day ='%s')>" % (
                             self.bid, self.sid, self.day)
    
class Boat(Base):
    
    __tablename__ = 'Boats'
    
    bid = Column(Integer, primary_key=True)
    bname = Column(String)
    color = Column(String)
    length = Column(Integer)
    
    def __repr__(self):
        return "<User(boat id='%d', boat name ='%s',color ='%s', length = '%d')>" % (
                             self.bid, self.bname, self.color,self.length)


Base.metadata.create_all(engine)


for(sid,sname,rating,age) in sailors:
    session.add(Sailor(sid = sid, sname = sname, rating = rating, age = age))


session.commit()

for(sid,bid,v) in reserves:
    session.add(Reservation(sid=sid,bid=bid,day=date(int(v[:4]), int(v[5:7]), int(v[8:10]))))

session.commit()

for (bid, bname, color, length) in boats:
    session.add(Boat(bid=bid, bname = bname, color = color, length = length))


session.commit()


from sqlalchemy.sql import select
from sqlalchemy import func
from sqlalchemy import case
from sqlalchemy import and_

conn = engine.connect()

expr = case(
        [
            ((Boat.color == "red"), 1),
        ],
        else_=0,
    )

def firstQueryTest(Boat,Reservation):
    assert len(session.query(Boat.bname,func.count(Boat.bname)).               select_from(Boat).join(Reservation, Boat.bid == Reservation.bid).               group_by(Boat.bname).all()) == 6

def secondQueryTest(Sailor,Boat,Reservation):
    
    subquery = session.query(func.count(Boat.bname)).    select_from(Boat).filter(Boat.color == "red").subquery()
    
    assert len(session.query(Sailor.sname,Sailor.sid).
     select_from(Reservation).join(Boat).join(Sailor).\
    group_by(Sailor.sname,Sailor.sid).\
    having(func.sum(expr) == subquery).all()) == 0

def thirdQueryTest(Sailor,Boat,Reservation):
    assert len(session.query(Sailor.sname,Sailor.sid).
    select_from(Reservation).join(Boat).join(Sailor).\
    group_by(Sailor.sname,Sailor.sid).\
    having(func.sum(expr) == func.count(Boat.color)).all()) == 5


def fourthQueryTest(Boat,Reservation):
    assert session.query(Boat.bname, func.count(Reservation.sid)).    select_from(Reservation).join(Boat).    group_by(Boat.bname).first() == ('Clipper', 8)
    
def fifthQueryTest(Sailor,Boat,Reservation):
    assert len(session.query(Sailor.sname,Sailor.sid).
    select_from(Reservation).join(Boat).join(Sailor).\
    group_by(Sailor.sname,Sailor.sid).\
    having(func.sum(expr) == 0).all()) == 3

def sixthQueryTest(Sailor):
    assert (session.query(func.avg(Sailor.age)).    select_from(Sailor).    filter(Sailor.rating == 10).all())[0][0] == 35.0
    
def seventhQueryTest(Sailor):
    x = []
    for i in range(len(session.query(Sailor.rating).distinct().all())):
        
        subquery = session.query(Sailor.age).        select_from(Sailor).        filter(Sailor.rating == session.query(Sailor.rating).distinct().all()[i][0]).        group_by(Sailor.age).first()
        
        
        x.append(session.query(Sailor.sname,Sailor.sid).        select_from(Sailor).        filter(and_(Sailor.rating == session.query(Sailor.rating).distinct().all()[i][0],Sailor.age.in_(subquery))).first())
    
    assert len(x) == 6

def eighthQueryTest(Sailor,Boat,Reservation):
    x = []
    for i in range(len(session.query(Boat.bname).distinct().all())):
        
        x.append(session.query(func.count(Reservation.bid),Boat.bname,Sailor.sname).        select_from(Reservation).join(Sailor).join(Boat).        filter(Boat.bname == session.query(Boat.bname).distinct().all()[i][0]).        group_by(Boat.bname,Sailor.sname).first())

    assert len(x) == 6    
    

firstQueryTest(Boat,Reservation)

secondQueryTest(Sailor,Boat,Reservation)

thirdQueryTest(Sailor,Boat,Reservation)

fourthQueryTest(Boat,Reservation)

fifthQueryTest(Sailor,Boat,Reservation)

sixthQueryTest(Sailor)

seventhQueryTest(Sailor)

eighthQueryTest(Sailor,Boat,Reservation)


