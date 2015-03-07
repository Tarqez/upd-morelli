# -*- coding: utf-8 -*-
import sys, csv, os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Unicode, Boolean


# DB def with Sqlalchemy
# ----------------------

db_file = os.path.join('db', 'db.sqlite')
engine = create_engine('sqlite:///'+db_file, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Art(Base):
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    mo_code = Column(Unicode, unique=True, index=True, nullable=False)
    itemid = Column(Unicode, default=u'')     
    
    qty = Column(Integer, default=0)
    prc = Column(Float, default=0.0)

    update_qty = Column(Boolean, default=False)
    update_prc = Column(Boolean, default=False)    

class Sequence(Base):
    __tablename__ = 'sequences'

    id = Column(Integer, primary_key=True)
    number = Column(Integer, default=0)    

Base.metadata.create_all(engine)

# A csv.DictWriter specialized with Fx csv
# ----------------------------------------

class EbayFx(csv.DictWriter):
    '''Subclass csv.DictWriter, define delimiter and quotechar and write headers'''
    def __init__(self, filename, fieldnames):
        self.fobj = open(filename, 'wb')
        csv.DictWriter.__init__(self, self.fobj, fieldnames, delimiter=';', quotechar='"')
        self.writeheader()
    def close(self,):
        self.fobj.close()
       
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()


# Constants
# ---------

DATA_PATH = os.path.join('data')
ACTION = '*Action(SiteID=Italy|Country=IT|Currency=EUR|Version=745|CC=UTF-8)' # smartheaders CONST


# Fruitful functions
# ------------------

def get_fname_in(folder):
    'Return the filename inside folder'

    t = os.listdir(folder)
    if len(t) == 0: 
        raise Exception('No file found')
    elif len(t) == 1:
        el = os.path.join(folder, t[0])
        if os.path.isfile(el):
            return el
        else:
            raise Exception('No file found')
    else:
        raise Exception('More files or folders found')

def fx_fname(action_name):
    'Build & return Fx filename with a sequence number suffix'

    seq = s.query(Sequence).first()
    if seq: seq.number += 1
    else: seq = Sequence(number=0)      
    s.add(seq)
    s.commit()
    
    return action_name+'_'+str(seq.number).zfill(4)+'.csv'

def it_en_prc(p):
    'Convert it to en price format string. Empty values default to 0.'
    p = p.strip()
    p = '0' if p=='' else float(p.replace('.','').replace(',','.'))
    return p    


# Void functions
# --------------

def ebay_link_n_check():
    '''Read Fx report "attivo" and perform on DB
        - overwriting itemid with a value or blank 
        - setting update_qty, update_prc to True or False
    finally 
        - check if there are ads out of DB
        - check if there are ads with OutOfStockControl=false'''

    folder = os.path.join(DATA_PATH, 'attivo_report')
    fname = get_fname_in(folder)

    # Reset all: itemid, update_prc and update_qty
    all_arts = s.query(Art)
    for art in all_arts:
        art.itemid = u''
        art.update_qty = False
        art.update_prc = False
        s.add(art)

    #
    for report_line in ebay_report_datasource(fname):
        try:
            art = s.query(Art).filter(Art.mo_code == report_line['mo_code']).first()
            if art: # exsits, check values
                if art.qty != report_line['qty']:
                    art.update_qty = True
                if abs(art.prc - report_line['prc']) > 0.05:
                    art.update_prc = True
                art.itemid = report_line['itemid']
                s.add(art)
            else: # not exsist, items out of DB
                print 'Inserzione:'+report_line['itemid'], 'cod. int.:'+report_line['mo_code'], u'non è nel mio DB'
                    
            if report_line['OutOfStockControl'] == 'false':
                print 'Inserzione:'+report_line['itemid'], 'cod. int.:'+report_line['mo_code'], 'ha OutOfStockControl=false'
            
        except ValueError:
            print 'rejected line:'
            print report_line
            print sys.exc_info()[0]
            print sys.exc_info()[1]
            print sys.exc_info()[2]

    os.remove(fname)
    s.commit()        


def print_stats():
    'Print stats from db'

    print 'Statistiche dopo questo aggiornamento'
    print '-------------------------------------', '\n'

    print 'Inserzioni attive su eBay', 'per un totale di pezzi'
    print 'Inserzioni con quantità pari a 0'
    print 'Articoli senza inserzione che potresti aggiungere su eBay', 'per un totale pezzi'


# Datasources
# -----------

def pq_datasource(ftxt):
    'Yield a dict of values from estrazione.xls'

    def price(a,b,c):
        'Return the max price'
        return max(float(a), float(b), float(c))/1000

    data_line = dict()

    with open(ftxt, 'rb') as f:
        dsource_rows = csv.reader(f, delimiter=';', quotechar='"')
        #dsource_rows.next()
        for row in dsource_rows:
            try:
                data_line['mo_code'] = row[0].strip()
                data_line['prc'] = price(row[2], row[3], row[4])
                data_line['qty'] = int(row[9])                            
                
                yield data_line
                
            except ValueError:
                print 'rejected line:'
                print row
                print sys.exc_info()[0]
                print sys.exc_info()[1]
                print sys.exc_info()[2]


def ebay_report_datasource(fcsv):
    'Yield a dict of values from ebay report attivo, mo_code included'

    ebay_report_line = dict()

    with open(fcsv, 'rb') as f:
        dsource_rows = csv.reader(f, delimiter=';', quotechar='"')
        dsource_rows.next()
        for row in dsource_rows:
            try:
                ebay_report_line['mo_code'] = row[1][:10]
                ebay_report_line['qty'] = int(row[5])
                ebay_report_line['prc'] = float(it_en_prc(row[8].replace('EUR', '')))
                ebay_report_line['itemid'] = row[0].strip()
                ebay_report_line['OutOfStockControl'] = row[22].lower()

                yield ebay_report_line
                
            except ValueError:
                print 'rejected line:'
                print row
                print sys.exc_info()[0]
                print sys.exc_info()[1]
                print sys.exc_info()[2] 


# loaders
# -------

def pq_loader():
    "Load prc & qty into DB"

    print 'Linking to pq_datasource ...', '\n'

    folder = os.path.join(DATA_PATH, 'estrazione')

    # link to datasource
    fname = get_fname_in(folder)
    pq = pq_datasource(fname)

    print '\n', 'Loading pq_datasource ...' 

    # set all qties to zero (missing datasource zero-qty rows hack)
    for art in s.query(Art):
        art.qty = 0
        s.add(art)
    s.commit()


    for data_line in pq:
        try:
            art = s.query(Art).filter(Art.mo_code == data_line['mo_code']).first()
            if not art: # not exsists, create
                art = Art()

            art.mo_code = data_line['mo_code']
            art.prc = data_line['prc']
            art.qty = data_line['qty']

            s.add(art)
        except ValueError:
            print 'rejected line:'
            print data_line
            print sys.exc_info()[0]
            print sys.exc_info()[1]
            print sys.exc_info()[2]
    s.commit()
    os.remove(fname)

    print_stats()


# FX csv file creators
# --------------------

def revise_qty():
    'Fx revise quantity action'
    smartheaders = (ACTION, 'ItemID', '*Quantity')
    arts = s.query(Art).filter(Art.itemid != u'', Art.update_qty)
    fout_name = os.path.join(DATA_PATH, fx_fname('revise_qty'))
    with EbayFx(fout_name, smartheaders) as wrt:
        for art in arts:
            fx_revise_row = {ACTION: 'Revise',
                             'ItemID': art.itemid,
                             '*Quantity': art.qty,}
            wrt.writerow(fx_revise_row)
            art.update_qty = False
            s.add(art)
        s.commit()


def revise_prc():
    'Fx revise price'
    smartheaders=(ACTION, 'ItemID', '*StartPrice')
    arts = s.query(Art).filter(Art.itemid != u'', Art.update_prc)
    fout_name = os.path.join(DATA_PATH, fx_fname('revise_prc'))
    with EbayFx(fout_name, smartheaders) as wrt:
        for art in arts:
            if art.prc > 1: # jump art if his price is less than 1 euro
                fx_revise_row = {ACTION: 'Revise',
                                 'ItemID': art.itemid,
                                 '*StartPrice': art.prc,}
                wrt.writerow(fx_revise_row)
            art.update_prc = False
            s.add(art)
        s.commit()    