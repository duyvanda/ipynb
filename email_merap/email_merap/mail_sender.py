import smtplib
from email.message import EmailMessage
from vars import vars

def sendmail():
    #Email connection
    EMAIL_ADDRESS = vars['name']
    EMAIL_INFORMATION = vars['information']
    msg = EmailMessage()
    # FROM EMAIL
    msg['From'] = EMAIL_ADDRESS
    # TO EMAIL
    msg['To'] = ['duyvq@merapgroup.com']
    # BODY
    msg.set_content('BI TEAM - Report has been generated')
    msg['Subject'] = 'BI TEAM - Report has been generated, please check your local files system'
    with smtplib.SMTP_SSL('smtp.merapgroup.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_INFORMATION)
        smtp.send_message(msg)
        print('Message has been sent')
