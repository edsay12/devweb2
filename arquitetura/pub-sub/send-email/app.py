from PIL import Image, ImageOps
import os
from confluent_kafka import Consumer, KafkaError,Producer
import json
import logging
from time import sleep
import time 
from uuid import uuid4


# Email 
import smtplib
from email.message import EmailMessage
from secret import EMAIL_ADRESS,EMAIL_PASSWORD

msg = EmailMessage()

# funçao que ira mandar o email 
def sendEmail(subject,content):
    msg = EmailMessage()
    msg['subject'] = subject
    msg['From'] = EMAIL_ADRESS
    msg['To'] = 'edvandearaujo2@hotmail.com'
    msg.set_content(content)

    with smtplib.SMTP_SSL('smtp.gmail.com',465) as smtp:
        smtp.login(EMAIL_ADRESS,EMAIL_PASSWORD)
        smtp.send_message(msg)




#sleep(30)
### Consumer
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'send-email',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notify'])
#{"timestamp": 1649288146.3453217, "new_file": "9PKAyoN.jpeg"}



try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            datatype = data['MensageType']
            
            sendEmail(f'Serviço {datatype} foi Usado ',f'A foto {filename} usou o serviço de {datatype}')
            
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
