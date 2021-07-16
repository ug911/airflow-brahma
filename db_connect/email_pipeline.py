"""
--> EmailPipeline
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from db_config import EMAIL, PASSWORD, SENDER


class EmailPipeline:
    """
    Email pipeline is a class that stores all the email sending related functions
    """
    @staticmethod
    def send_mail_without_attachment(subject, body, receiver_list: list):
        """
        Send an email without attachment
        Args:
            subject ():
            body ():
            receiver_list ():

        Returns:

        """
        for receiver in receiver_list:
            print("Receiver is ", receiver)
            msg = MIMEMultipart()
            msg['From'] = SENDER
            msg['To'] = receiver
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
            smtp_server.starttls()
            smtp_server.login(EMAIL, PASSWORD)
            text = msg.as_string()
            smtp_server.sendmail(SENDER, receiver, text)
            smtp_server.quit()

    @staticmethod
    def send_mail_with_attachment(subject, body, receiver_list: list, output_file):
        """
        Send an email with attachment
        Args:
            subject ():
            body ():
            receiver_list ():
            output_file ():

        Returns:

        """
        for receiver in receiver_list:
            print("Receiver is ", receiver)
            msg = MIMEMultipart()
            msg['From'] = SENDER
            msg['To'] = receiver
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            file_name = output_file.split('/')
            file_name = file_name[len(file_name) - 1]

            attachment = open(output_file, "rb")
            payload = MIMEBase('application', 'octet-stream')
            payload.set_payload(attachment.read())
            encoders.encode_base64(payload)
            payload.add_header('Content-Disposition', "attachment; filename= %s" % file_name)

            msg.attach(payload)
            smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
            smtp_server.starttls()
            smtp_server.login(EMAIL, PASSWORD)
            text = msg.as_string()
            smtp_server.sendmail(SENDER, receiver, text)
            smtp_server.quit()
