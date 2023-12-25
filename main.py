import ConfigParser
import time
import logging
from db import PsqlDB
from zabbix import Zabbix
from datetime import datetime, timedelta

def main():
    log_path = '/var/log/SQLparser.log'
    config_path = '/opt/SQLparser/SQLparser.conf'

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        filename=log_path,
        filemode='a'
    )

    config = ConfigParser.ConfigParser()
    if os.path.isfile(file_path):
        config.read('SQLparser.conf')
    else:
        logging.error('No config found')
        raise Exception('The configuration file was not found')

    dbuser = config.get('PSQL', 'username')
    dbname = config.get('PSQL', 'dbname')
    dbtable = config.get('PSQL', 'dbtable')
    if not all([dbuser, dbname, dbtable]):
        logging.error('One or more configuration variables are not defined in SQLparser.conf')
        raise Exception("Error while reading PSQL config")

    zuser = config.get('ZABBIX', 'username')
    zpasswd = config.get('ZABBIX', 'userpasswd')
    zhost = config.get('ZABBIX', 'host')
    zserver = config.get('ZABBIX', 'server')
    if not all([zuser, zpasswd, zhost, zserver]):
        logging.error('One or more configuration variables are not defined in SQLparser.conf')
        raise Exception('Error while reading ZABBIX config')
    
    try:
        db = PsqlDB(dbname=dbname, user=dbuser)
    except Exception as e:
        logging.error('Unable connect to the database: {}'.format(e))
        raise

    try:
        zabbix = Zabbix(host=zhost, server=zserver, user=zuser, passwd=zpasswd, localhost=True)
    except Exception as e:
        logging.error('Unable connect to zabbix')
        raise
    
    sflow_data = {}
    end_time = datetime.now()
    start_time = end_time - timedelta(seconds=30)

    try:
        sflow_data['bytes'] = db.get_bytes(start_time, end_time)
        sflow_data['packets'] = db.get_packets(start_time, end_time)
    except Exception as e:
        logging.error('Unable get data from database')
        raise

    current_timestamp = int(time.mktime(datetime.now().timetuple()))
    try:
        zabbix.send_to_zabbix('sflow_bytes', current_timestamp, sflow_data['bytes'])
        zabbix.send_to_zabbix('sflow_packets', current_timestamp, sflow_data['packets'])
    except Exception as e:
        logging.error('Unable send data to zabbix')
        raise
        
    db.close_conn()
    
if __name__ == "__main__":
    main()