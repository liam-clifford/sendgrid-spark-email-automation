from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import datetime
from datetime import timedelta, timedelta
import numpy as np
import re
from time import sleep
import pandas as pd

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp
    
def send_email_notification(mode,
                            pandas_email_df, 
                            email_body_template_html, 
                            email_subject, 
                            from_user_email, 
                            sendgrid_key, 
                            notification_type=None,
                            update_historical_notification_log=False,
                            number_of_test_records=1,
                            do_not_send_any_emails=False,
                            skip_if_email_sent=False,
                            historical_database_table=None,
                            bcc_emails=None,
                            **kwargs):
    """
    
    Automate the process of sending emails via SendGrid and logging the email records using Spark.
    Args:
        mode (str): The mode in which the function is running - 'test' or 'prod'.
        pandas_email_df (pandas.DataFrame): A Pandas DataFrame that contains the email recipients' details. It should 
                                            at least have the following columns: `to_user_emails`, `cc_user_emails`, 
                                            `unique_id`. 
        email_body_template_html (str): The HTML string template for the email body.
        email_subject (str): The email subject.
        from_user_email (str): The email address from which the email will be sent.
        sendgrid_key (str): SendGrid API key.
        notification_type (str): The type of notification.
        update_historical_notification_log (bool): Whether to update the historical notification log.
        historical_database_table (str): The name of the historical database table.
        number_of_test_records (int): The number of test records to send.
        do_not_send_any_emails (bool): Whether to send any emails.
        bcc_emails (list): The list of email addresses to bcc the email.
        skip_if_email_sent (bool): Whether to skip sending the email if it has already been sent.
        
        **kwargs:
            email_body_variables (list): A list of column names from pandas_email_df to use as variables 
                                                 in the email body template.
            email_subject_variables (list): A list of column names from pandas_email_df to use as variables 
                                                 in the email subject.
            only_send_to_test_emails (list): A list of email addresses to which the email will be sent if 
                                                 mode is 'test'.
            reply_to_domain (str): If you have set up Domain Authentication for your SendGrid account, 
                                   you must provide this argument to ensure that recipients can reply to the correct domain.
            do_not_cc_anyone (bool): Whether to cc anyone.
            
    """

    spark = SparkSession \
        .builder \
        .appName("Spark") \
        .getOrCreate()
    # OPTIONAL IF USING Databricks

    print_spacing = 30
    
    def build_email_body(email_dict, email_body_template_html, i, kwargs):
        if kwargs is not None and 'email_body_variables' in kwargs:
            email_body_variables_list = [email_dict['records'][i][x] for x in kwargs['email_body_variables']]
            print(f"\n{'email_body_variables:':<{print_spacing}}{email_body_variables_list}")
            email_html = email_body_template_html.format(*email_body_variables_list)
        else:
            email_html = email_body_template_html
        return email_html, email_body_template_html
      
      
    def build_email_subject(email_dict, email_subject, i, kwargs):
        if kwargs is not None and 'email_subject_variables' in kwargs:
            email_subject_variables_list = [email_dict['records'][i][x] for x in kwargs['email_subject_variables']]
            email_subject_variables = kwargs['email_subject_variables']
            print(f"{'email_subject_variables:':<{print_spacing}}{email_subject_variables}")
            emailSubject = email_subject.format(*email_subject_variables_list)
            print(f"{'email_subject:':<{print_spacing}}{emailSubject}")
        else:
            emailSubject = email_subject
            print(f"{'emailSubject:':<{print_spacing}}{emailSubject}")

        return emailSubject
    
    
    def build_email_addresses(to_user_emails, kwargs):
        if kwargs is not None and 'only_send_to_test_emails' in kwargs:
            if isinstance(kwargs['only_send_to_test_emails'], list)==False:
              raise ValueError('\n',f"`only_send_to_test_emails` needs to be a python list.")
                  
            to_user_emails = kwargs['only_send_to_test_emails']
            print(f"\n{'Sending To: ':<{print_spacing}}{to_user_emails}")
            to_emails = [To(x) for x in to_user_emails]
        else:
            to_emails = [To(x) for x in str(to_user_emails).split(',')]
            print(f"\n{'Sending To: ':<{print_spacing}}{[x for x in str(to_user_emails).split(',')]}")

        return to_emails

      
    def add_additional_recipients(cc_user_emails, bcc_user_emails, message, kwargs):
        if kwargs is not None and 'only_send_to_test_emails' in kwargs:
            pass
        else:
            if str(cc_user_emails).find('@') != -1:
                if kwargs is not None and 'do_not_cc_anyone' in kwargs:
                    pass
                else:
                    print(f"""\n{"CC'ing:":<{print_spacing}}{[x for x in str(cc_user_emails).split(',')]}""")
                    for a in np.arange(0,int(str(cc_user_emails).count(','))+1):
                        message.personalizations[0].add_cc(Email(str(cc_user_emails).split(',')[a]))

            if str(bcc_user_emails).find('@') != -1:
                print(f"""\n{"BCC'ing:":<{print_spacing}}{[x for x in str(bcc_user_emails).split(',')]}""")
                for a in np.arange(0,int(str(bcc_user_emails).count(','))+1):
                    message.personalizations[0].add_bcc(Email(str(bcc_user_emails).split(',')[a]))

        return message
    
    
    def build_message(from_user_email, to_user_emails, email_subject, email_html):

        message = Mail(
            from_email=from_user_email,
            to_emails=to_user_emails,
            subject=email_subject,
            html_content=email_html
        )
        
        if kwargs is not None and 'reply_to_domain' in kwargs: 
            replyTo = from_user_email.split('@')[0] + '@' + kwargs['reply_to_domain']
            message.reply_to = ReplyTo(replyTo,replyTo)

        return message

      
    def send_email(message, sendGridKey):
            try:
                sg = SendGridAPIClient(sendGridKey)
                response = sg.send(message)
                print(response.status_code)
                print(response.body)
                print(response.headers)
            except Exception as e:
                print(e)

                
    def historical_notification_table_existence_check(historical_database_table):
        if not spark.catalog.tableExists(f"{historical_database_table}"):
            print(f'\nCreating {historical_database_table} in {spark.conf.get("spark.sql.catalogImplementation")} catalog')
            # Define the schema
            schema = StructType([
                StructField('to_user_emails', StringType(), True),
                StructField('cc_user_emails', StringType(), True),
                StructField('notification_type', StringType(), True),
                StructField('unique_id', StringType(), True),
                StructField('datetime_sent', TimestampType(), True)
            ])

            # Create an empty DataFrame with the defined schema
            empty_df = spark.createDataFrame([], schema)

            # Save the empty DataFrame as a table
            empty_df.write.saveAsTable(historical_database_table)
            print('SUCCESSFULLY CREATED')
            
                
    def append_to_delta_table(data,historical_database_table):
        cols = ['to_user_emails', 'cc_user_emails', 'notification_type', 'unique_id']
        email_df = pd.DataFrame(columns=cols, data=data)
        df = spark.createDataFrame(email_df).withColumn('datetime_sent', current_timestamp())
        
        # check to see if table exists or if we need to create it
        historical_notification_table_existence_check(historical_database_table)
        
        database_name,table_name = historical_database_table.split('.')[0],historical_database_table.split('.')[1]
        table_path = spark.sql(f"""describe detail {database_name}.{table_name}""").toPandas()['location'].values[0]
        
        while True:
            try:
                df.write.format("delta").mode("append").save(f"{table_path}")
                break  # Break out of the while loop if append succeeds
            except Exception as e:
                print(f"Append to Delta table failed with error: {e}")
                sleep(10)  # Wait for 10 seconds before retrying the append operation

                
    def create_dicts_from_df(pandas_email_df, number_of_test_records, notification_type=None):
        loop_details_dict = {'records': {}}
        final_pandas_df = pd.DataFrame(pandas_email_df, columns=pandas_email_df.columns)

        for i, row in final_pandas_df.iterrows():
            row_dict = row.to_dict()
            
            if notification_type:
              row_dict['notification_type'] = notification_type
              
            loop_details_dict['records'][i] = row_dict

        test_dict = {'records': {}}
        for i in range(0, number_of_test_records):
            try:
                test_dict['records'][i] = loop_details_dict['records'][i]
            except Exception as e:
                print(f'error message: {e}')
                first, second = number_of_test_records, len(test_dict['records'])
                print(f'the above error is likely due to your `number_of_test_records` ({first}) value exceeds the # of rows in `pandas_email_df` ({second})')

        return loop_details_dict, test_dict
      
    def filter_pandas_dataframe(pandas_email_df: pd.DataFrame, historical_database_table: str, notification_type: str) -> pd.DataFrame:
        cols = pandas_email_df.columns
        assert 'to_user_emails' in cols, f"Error: 'to_user_emails' is not a column in `pandas_email_df`"
        assert 'cc_user_emails' in cols, f"Error: 'cc_user_emails' is not a column in `pandas_email_df`"
        assert 'unique_id' in cols, f"Error: 'unique_id' is not a column in `pandas_email_df`"

        # check to see if table exists or if we need to create it
        historical_notification_table_existence_check(historical_database_table)

        # Load the historical table as a pandas DataFrame
        historical_pandas_table = spark.table(historical_database_table).toPandas()

        # Add notification_type as column
        pandas_email_df['notification_type'] = notification_type
        
        # Filter the pandas_email_df based on the to_user_emails, unique_id, and notification_type columns
        filtered_pandas_email_df = pandas_email_df[~pandas_email_df[['to_user_emails', 'unique_id', 'notification_type']].\
                                   apply(lambda x: tuple(map(str, x)), axis=1).\
                                   isin(historical_pandas_table[['to_user_emails', 'unique_id', 'notification_type']].\
                                   apply(lambda x: tuple(map(str, x)), axis=1))]

        return filtered_pandas_email_df


      
    historical_data_list = []
    # always set `historical_data_list` to empty
    
    if (update_historical_notification_log or skip_if_email_sent or historical_database_table!=None) and notification_type==None:
        assert notification_type, f"\nError: please provide a value for the 'notification_type' argument"
    
    if skip_if_email_sent:
        assert historical_database_table, f"\nError: please provide a value for the 'historical_database_table' argument"
        print(f'\nExclude any rows where `unique_id` and `notification_type` ({notification_type}) combination exist in `{historical_database_table}`')
        pandas_email_df = filter_pandas_dataframe(pandas_email_df, historical_database_table, notification_type)
                
    email_dict, test_dict = create_dicts_from_df(pandas_email_df, number_of_test_records, notification_type)

    if isinstance(mode, str)==False or (mode != 'test' and mode != 'prod'):
        raise ValueError("""
       `mode` does not equal `test` or `prod` or is not a str
       --> please set it to `prod` or `test`
       -`prod`: sends to whomever is contained in the `to_user_emails` value (for each row in `pandas_email_df`)
       -`test`: will only send to the email(s) in `only_send_to_test_emails` (for each row in `pandas_email_df`)""")
     
    if mode == 'prod' and 'only_send_to_test_emails' in kwargs:
        print(f'\nRemoving `only_send_to_test_emails` from **kwargs because `mode`==`{mode}`')
        del kwargs['only_send_to_test_emails']

    if mode == 'test' \
        and ((kwargs is not None and 'only_send_to_test_emails' not in kwargs) or \
             (kwargs is not None and 'only_send_to_test_emails' in kwargs and isinstance(kwargs['only_send_to_test_emails'], list)==False)):
        raise ValueError("""
         `mode` is set to 'test'
         --> 'test' mode requires you to set and format `only_send_to_test_emails` as a python *list* that includes at least 1 email address.""")
    
    if mode == 'test':

      print('\n--> setting `email_dict` == `test_dict`')
      print(f"""`test_dict` contains {number_of_test_records} {'record' if number_of_test_records == 1 else 'records'} from `pandas_email_df`""")
      print('\n')
      
      email_dict = test_dict
    
    num_emails = len(email_dict.get('records', []))

    if num_emails > 0: 
        for i in range(0,num_emails):
        
            assert 'to_user_emails' in email_dict['records'][i], f"Error: 'to_user_emails' key not found in email_dict['records'][{i}]"
            assert 'cc_user_emails' in email_dict['records'][i], f"Error: 'cc_user_emails' key not found in email_dict['records'][{i}]"
            assert 'unique_id' in email_dict['records'][i], f"Error: 'unique_id' key not found in email_dict['records'][{i}]"
            assert 'notification_type' in email_dict['records'][i], f"Error: 'notification_type' key not found in email_dict['records'][{i}]"
            
            to_user_emails = ','.join([x for x in str(email_dict['records'][i]['to_user_emails']).split(',') if x.find('@')!=-1])
            
            cc_users = [x for x in str(email_dict['records'][i]['cc_user_emails']).split(',') if x.find('@')!=-1 and x not in to_user_emails.split(',')]
            
            if len(cc_users)>0:
                cc_user_emails = ','.join(cc_users)
            else:
                cc_user_emails = None
            
            if bcc_emails is not None:
                if isinstance(bcc_emails, list)==False:
                    raise ValueError('\n',f"`bcc_emails` needs to be a python list.")
                  
                bcc_users = [x for x in bcc_emails if x.find('@') != -1 and x not in to_user_emails.split(',')]
                
                if len(bcc_users)>0:
                    bcc_user_emails = ','.join([x for x in bcc_emails if x.find('@') != -1 and x not in to_user_emails.split(',')])
                else:
                    bcc_user_emails = None
            else:
                bcc_user_emails = None

            notification_type = str(email_dict['records'][i]['notification_type'])
            unique_id = str(email_dict['records'][i]['unique_id'])

            historical_data_list.append([to_user_emails,\
                                        cc_user_emails,\
                                        notification_type,\
                                        unique_id])

            email_html, email_body_template_html = build_email_body(email_dict, email_body_template_html, i, kwargs)
            email_subject = build_email_subject(email_dict, email_subject, i, kwargs)
            to_user_emails = build_email_addresses(to_user_emails, kwargs)
            message = build_message(from_user_email, to_user_emails, email_subject, email_html)
            message = add_additional_recipients(cc_user_emails, bcc_user_emails, message, kwargs)
            
            if do_not_send_any_emails==False:
                send_email(message, sendgrid_key)
                print(f'\n{i+1}) email status: SUCCESS')
            else:
                print(f'\n{i+1}) do_not_send_any_emails: `{do_not_send_any_emails}` -> do not send email')
            
        if update_historical_notification_log:
            assert historical_database_table, f"Error: please provide a value for the 'historical_database_table' argument"
            append_to_delta_table(data=historical_data_list,historical_database_table=historical_database_table)
            print(f"\n--> Added {len(historical_data_list)} {'record' if len(historical_data_list) == 1 else 'records'} to {historical_database_table}")
        else:
            print(f"\n--> {'update_historical_notification_log: ':<{print_spacing}}{update_historical_notification_log}")

    else:
        print('\nNo data found in `email_dict` variable => No emails to send')
