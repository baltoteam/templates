import argparse
import httplib2
import os
import oauth2client
from oauth2client import client, tools
import base64
from email import encoders

import smtplib  
import mimetypes
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from apiclient import errors, discovery

# SCOPES = 'https://www.googleapis.com/auth/gmail.send'
# SCOPES = 'https://mail.google.com/'
SCOPES = 'https://mail.google.com/'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Gmail API Python Send Email'


def get_credentials():
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'gmail-python-email-send.json')
    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run_flow(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def create_message_and_send(sender, to, subject,  message_text_plain, message_text_html, attached_files=None):
    credentials = get_credentials()
    http = httplib2.Http()
    http = credentials.authorize(http)

    service = discovery.build('gmail', 'v1', http=http)

    if attached_files is None:
        message_without_attachment = create_message_without_attachment(sender, to, subject, message_text_html, message_text_plain)
        send_Message_without_attachement(service, "me", message_without_attachment, message_text_plain)

    else:
        message_with_attachment = create_Message_with_attachment(sender, to, subject, message_text_plain, message_text_html, attached_files)
        send_Message_with_attachement(service, "me", message_with_attachment, message_text_plain,attached_files)


def create_message_without_attachment(sender, to, subject, message_text_html, message_text_plain):
    #Create message container
    message = MIMEMultipart('alternative') # needed for both plain & HTML (the MIME type is multipart/alternative)
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = to

    #Create the body of the message (a plain-text and an HTML version)
    message.attach(MIMEText(message_text_plain, 'plain'))
    # message.attach(MIMEText(message_text_html, 'html'))

    raw_message_no_attachment = base64.urlsafe_b64encode(message.as_bytes())
    raw_message_no_attachment = raw_message_no_attachment.decode()
    body  = {'raw': raw_message_no_attachment}
    return body


def create_Message_with_attachment2(sender, to, subject, message_text_plain, message_text_html, attached_files):
    message = MIMEMultipart() #when alternative: no attach, but only plain_text
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    message.attach(MIMEText(message_text_plain))
    # message.attach(MIMEText(message_text_html, 'html'))

    for attached_file in attached_files:
        my_mimetype, encoding = mimetypes.guess_type(attached_file)
        if my_mimetype is None or encoding is not None:
            my_mimetype = 'application/octet-stream' 

        main_type, sub_type = my_mimetype.split('/', 1)# split only at the first '/'
        if main_type == 'text':
            print("text")
            temp = open(attached_file, 'r')  # 'rb' will send this error: 'bytes' object has no attribute 'encode'
            attachement = MIMEText(temp.read(), _subtype=sub_type)
            temp.close()

        elif main_type == 'image':
            print("image")
            temp = open(attached_file, 'rb')
            attachement = MIMEImage(temp.read(), _subtype=sub_type)
            temp.close()

        elif main_type == 'audio':
            print("audio")
            temp = open(attached_file, 'rb')
            attachement = MIMEAudio(temp.read(), _subtype=sub_type)
            temp.close()            

        elif main_type == 'application' and sub_type == 'pdf':   
            temp = open(attached_file, 'rb')
            attachement = MIMEApplication(temp.read(), _subtype=sub_type)
            temp.close()

        else:                              
            attachement = MIMEBase(main_type, sub_type)
            temp = open(attached_file, 'rb')
            attachement.set_payload(temp.read())
            temp.close()

        encoders.encode_base64(attachement)  #https://docs.python.org/3/library/email-examples.html
        filename = os.path.basename(attached_file)
        attachement.add_header('Content-Disposition', 'attachment', filename=filename) # name preview in email
        message.attach(attachement)

    # message_as_bytes = message.as_bytes() # the message should converted from string to bytes.
    # message_as_base64 = base64.urlsafe_b64encode(message_as_bytes) #encode in base64 (printable letters coding)
    # raw = message_as_base64.decode()  # need to JSON serializable (no idea what does it means)
    # return {'raw': raw}
    encoded = base64.urlsafe_b64encode(message.as_string().encode('UTF-8')).decode('UTF-8') 
    return {'raw': encoded}


def send_Message_without_attachement(service, user_id, body, message_text_plain):
    try:
        message_sent = (service.users().messages().send(userId=user_id, body=body).execute())
        message_id = message_sent['id']
    except errors.HttpError as error:
        print (f'An error occurred: {error}')


def send_Message_with_attachement(service, user_id, message_with_attachment, message_text_plain, attached_files):
    try:
        message_sent = (service.users().messages().send(userId=user_id, body=message_with_attachment).execute())
        message_id = message_sent['id']
    except errors.HttpError as error:
        print (f'An error occurred: {error}')


def CreateDraft(service, user_id, message_body):
    message = {'message': message_body}
    draft = service.users().drafts().create(userId=user_id, body=message).execute()
    return draft


def create_Message_with_attachment(sender, to, subject, plain_text, attached_files):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = to

    for attached_file in attached_files:
        my_mimetype, encoding = mimetypes.guess_type(attached_file)
        if my_mimetype is None or encoding is not None:
            my_mimetype = 'application/octet-stream' 

        main_type, sub_type = my_mimetype.split('/', 1)# split only at the first '/'
        if main_type == 'text':
            print("text")
            temp = open(attached_file, 'r')  # 'rb' will send this error: 'bytes' object has no attribute 'encode'
            attachement = MIMEText(temp.read(), _subtype=sub_type)
            temp.close()

        elif main_type == 'image':
            print("image")
            temp = open(attached_file, 'rb')
            attachement = MIMEImage(temp.read(), _subtype=sub_type)
            temp.close()

        elif main_type == 'audio':
            print("audio")
            temp = open(attached_file, 'rb')
            attachement = MIMEAudio(temp.read(), _subtype=sub_type)
            temp.close()            

        elif main_type == 'application' and sub_type == 'pdf':   
            temp = open(attached_file, 'rb')
            attachement = MIMEApplication(temp.read(), _subtype=sub_type)
            temp.close()

        else:                              
            attachement = MIMEBase(main_type, sub_type)
            temp = open(attached_file, 'rb')
            attachement.set_payload(temp.read())
            temp.close()

        encoders.encode_base64(attachement)  #https://docs.python.org/3/library/email-examples.html
        filename = os.path.basename(attached_file)
        attachement.add_header('Content-Disposition', 'attachment', filename=filename) # name preview in email
        msg.attach(attachement)
#     html_text = """\
# <html>
#   <head></head>
#   <body>
#     <p>HTML email message</p>
#     <ol>
#         <li>As easy as one</li>
#         <li>two</li>
#         <li>three!</li>
#     </ol>
#     <p>Includes <a href="http://stackoverflow.com/">linktacular</a> goodness</p>
#   </body>
# </html>
# """
    # msg.attach(MIMEText(plain_text)) 
    msg.attach(MIMEText(plain_text))
    encoded = base64.urlsafe_b64encode(msg.as_string().encode('UTF-8')).decode('UTF-8') 
    return {'raw': encoded}


def send_draft():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)

    my_address = 'customerservice@balto.com.au' # Obscured to protect the culpable

    test_message = CreateTestMessage(sender=my_address,
                                 to='emma@balto.com.au',
                                 subject='Subject line Here',
                                 plain_text="Text email message.\nIt's plain.\nIt's text.")

    draft = CreateDraft(service, my_address, test_message)
    service.users().drafts().send(userId=my_address, body=draft).execute()


def main(to, from_, subject, msgHtml, msgPlain, attachment_paths):
    create_message_and_send(from_, to, subject, msgPlain, msgHtml, attachment_paths)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--to", type=str, required=True)
    parser.add_argument("-f", "--from_", type=str, required=True)
    parser.add_argument("-s", "--subject", type=str, required=True)
    # parser.add_argument("-m", "--msg", type=str, required=True,
    #                     help="Type your message. Use '\ n' to start a new line.")
    parser.add_argument("-a", "--attachment", type=str, required=False, nargs="+")

    args = parser.parse_args()

    to = args.to
    from_ = args.from_
    subject = args.subject
    # msgPlain = args.msg
    msgPlain = "Hi Helena\nWhat's up?\nHere are 2 random files.\nCheers"
    msgHtml = msgPlain.replace("\n", "<br/>")
    attachment_paths = args.attachment
    print(attachment_paths)
    main(to, from_, subject, msgHtml, msgPlain, attachment_paths)
