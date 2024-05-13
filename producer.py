import os
import time
import json
from bson import json_util
from kafka import KafkaProducer
from krbticket import KrbConfig, KrbCommand

#os.environ['KRB5CCNAME'] = '/tmp/krb5cc_<myusername>'
kconfig = KrbConfig(principal='kafka/cncbicdp01.asl.com.hk@BDA.COM', keytab='/root/python_kafka/kafka.keytab')
KrbCommand.kinit(kconfig)

# Kafka broker
BROKERS = ['cncbicdp01:9092','cncbicdp02:9092','cncbicdp03:9092']

# Kafka topics
TOPIC = 'smart_home_iot_source'

with open('smart_home_data.json') as json_file:
    iot_data = json.load(json_file)

print('loaded all data')

producer = KafkaProducer(
               bootstrap_servers=BROKERS,
               security_protocol='SASL_PLAINTEXT',
               sasl_mechanism='GSSAPI',
               sasl_kerberos_service_name='kafka',
           )

for i, event in enumerate(iot_data):
        producer.send(TOPIC, json.dumps(event, default=json_util.default).encode('utf-8'))
        producer.flush()

        if i % 10 == 0:
                print(f"sent {i} messages to kafka topic!")

        time.sleep(0.5)

print('program complete')
