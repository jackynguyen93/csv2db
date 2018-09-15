# -*- coding: utf-8 -*-
'''
Script:
    webapp.py
Description:
    Web service to get list member of telegram channel/group
Author:
    Nguyen Van
Creation date:
    06 Mar 2018
Last modified date:
    19 Apr 2018
'''

####################################################################################################

### Libraries/Modules ###

import configparser
import json

import pandas as pd
from cffi.backend_ctypes import xrange
from flask import Flask, request, render_template
from flask_cors import CORS
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Column
from sqlalchemy.sql import insert
from sqlalchemy.types import String, BIGINT

####################################################################################################

### Global variable ###
config = configparser.ConfigParser()
config.read('config/app.conf')
DB_HOST = config.get("BOT_DATABASE_CONFIG", "host")
DB_username = config.get("BOT_DATABASE_CONFIG", "username")
DB_password = config.get("BOT_DATABASE_CONFIG", "password")
engine = create_engine('mysql+pymysql://' + DB_username + ':' + DB_password + '@' + DB_HOST + '?charset=utf8mb4')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'somethingsectret'
CORS(app)
Base = declarative_base()


####################################################################################################
class JsonModel(object):
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


##### Table Def ############
class Store(Base, JsonModel):
    __tablename__ = 'store'
    __table_args__ = {'mysql_charset': 'utf8mb4'}
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    sales_record_number = Column(String(300))
    user_id = Column(String(300))
    buyer_fullname = Column(String(300))
    buyer_phone_number = Column(String(300))
    buyer_email = Column(String(300))
    buyer_address_1 = Column(String(300))
    buyer_address_2 = Column(String(300))
    buyer_city = Column(String(300))
    buyer_state = Column(String(300))
    buyer_postcode = Column(String(300))
    buyer_country = Column(String(300))
    item_number = Column(String(300))
    item_title = Column(String(300))
    custom_label = Column(String(300))
    quantity = Column(String(300))
    sale_price = Column(String(300))
    postage_and_handling = Column(String(300))
    insurance = Column(String(300))
    cash_on_delivery_fee = Column(String(300))
    total_price = Column(String(300))
    payment_method = Column(String(300))
    sale_date = Column(String(300))
    checkout_date = Column(String(300))
    paid_on_date = Column(String(300))
    posted_on_date = Column(String(300))
    feedback_left = Column(String(300))
    feedback_received = Column(String(300))
    notes_to_yourself = Column(String(300))
    unique_product_id = Column(String(300))
    private_field = Column(String(300))
    product_id_type = Column(String(300))
    product_id_value = Column(String(300))
    product_id_value_2 = Column(String(300))
    paypal_transaction_id = Column(String(300))
    postage_service = Column(String(300))
    cash_on_delivery_option = Column(String(300))
    transaction_on_id = Column(String(300))
    order_id = Column(String(300))
    variation_details = Column(String(300))
    global_shipping_program = Column(String(300))
    shipping_reference_id = Column(String(300))
    click_and_collect = Column(String(300))
    collect_reference = Column(String(300))
    post_to_address_1 = Column(String(300))
    post_to_address_2 = Column(String(300))
    post_to_city = Column(String(300))
    post_to_state = Column(String(300))
    post_to_postal_code = Column(String(300))
    post_to_country = Column(String(300))
    ebay_plus = Column(String(300))
    store_id = Column(String(300))


####################################################################################################

def parse_multi_form(form):
    data = {}
    for url_k in form:
        v = form[url_k]
        ks = []
        while url_k:
            if '[' in url_k:
                k, r = url_k.split('[', 1)
                ks.append(k)
                if r[0] == ']':
                    ks.append('')
                url_k = r.replace(']', '', 1)
            else:
                ks.append(url_k)
                break
        sub_data = data
        for i, k in enumerate(ks):
            if k.isdigit():
                k = int(k)
            if i+1 < len(ks):
                if not isinstance(sub_data, dict):
                    break
                if k in sub_data:
                    sub_data = sub_data[k]
                else:
                    sub_data[k] = {}
                    sub_data = sub_data[k]
            else:
                if isinstance(sub_data, dict):
                    sub_data[k] = v

    return data

####################################################################################################

@app.route('/get-db-headers', methods=['GET'])
def getDbHeaders():
    response = app.response_class(
        response=json.dumps([column.key for column in Store.__table__.columns]),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/')
def home():
    return render_template('index.html', message='')


@app.route('/get-data', methods=['GET'])
def getDb():
    Session = scoped_session(session_factory)
    session = Session()
    return json.dumps([u.as_dict() for u in session.query(Store).all()])


@app.route('/upload', methods=['POST'])
def upload():
    uploadFile = request.files['data_file']
    res = "OK"
    if not uploadFile:
        res = "No file"
    colMap = parse_multi_form(request.form)['col']
    df = pd.read_csv(uploadFile, encoding='windows-1252')
    df.drop(df.tail(2).index,inplace=True)
    data = df.to_dict()
    Session = scoped_session(session_factory)
    session = Session()
    length = len(list(data.values())[0])
    for i in xrange(length):
        valueMapping = {}
        for col in colMap:
            valueMapping[col] = str(data[colMap[col]][i]) if data[colMap[col]][i] == data[colMap[col]][i] else None
            if col == 'custom_label' and data[colMap[col]][i] == data[colMap[col]][i]:
                valueMapping['store_id'] = data[colMap[col]][i].split("-")[0]
        ins = insert(Store)
        ins = ins.values(valueMapping)
        session.execute(ins)
    session.commit()
    response = app.response_class(
        response=json.dumps(res),
        status=200,
        mimetype='application/json'
    )
    return response


####################################################################################################
if __name__ == '__main__':
    connection = engine.connect()
    session_factory = sessionmaker(bind=engine)
    Store.__table__.create(bind=engine, checkfirst=True)
    app.run(port=3001)
    connection.close()
