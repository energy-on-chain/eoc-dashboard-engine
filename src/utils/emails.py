###############################################################################
# PROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE: 4-August-2022
# FILENAME: emails.py
# DESCRIPTION: Utility functions for sending emails.
###############################################################################
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email_without_attachment(subject, body, footer, params):

    # Authenticate
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(params['sender_email'], params['2fapassword'])

    # Build message
    for receiver in params['receiver_email_list']:
        message = MIMEMultipart()
        message['From'] = params['sender_email']
        message['To'] = receiver
        message['Subject'] = subject
        message.attach(MIMEText(body + footer, 'plain'))

        text = message.as_string()
        server.sendmail(params['sender_email'], receiver, text)
        server.sendmail(params['sender_email'], receiver, message + params['tagline'])

    # Shutdown
    server.quit()


def send_email_with_attachment(subject, body, footer, attachment, params):

    # Authenticate
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(params['sender_email'], params['2fapassword'])

    # Build message
    for receiver in params['receiver_email_list']:
        message = MIMEMultipart()
        message['From'] = params['sender_email']
        message['To'] = receiver
        message['Subject'] = subject
        message.attach(MIMEText(body + footer, 'plain'))
        message.attach(attachment)

        text = message.as_string()
        server.sendmail(params['sender_email'], receiver, text)
        server.sendmail(params['sender_email'], receiver, message + params['tagline'])

    # Shutdown
    server.quit()

