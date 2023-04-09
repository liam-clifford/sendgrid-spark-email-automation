# sendgrid-spark-email-automation
`send_email_notification` is a wrapper function, built on top of the [SendGrid](https://github.com/sendgrid/sendgrid-python) Python library, that aims to make it easier to send custom email notifications via Sendgrid/record them in a Database through Spark. It also provides various optional parameters that allow users to personalize the recipient list, subject line, and email body.

# Table of Contents
- [Shameless Plug](#shameless-plug)
- [Prerequisites](#prerequisites)
- [Dependencies](#dependencies)
- [Setup](#setup)
  - [SendGrid](#sendgrid)
    - [Generate SendGrid API Key](#generate-sendgrid-api-key)
    - [Sender Verification](#sender-verification)
        - [Single Sender Verification (Option 1)](#1-single-sender-verification)
        - [Domain Verification (Option 2)](#2-domain-verification)
  - [Clone the repository](#clone-the-repository)
  - [Install the library using pip](#install-the-library-using-pip)
  - [Import Module](#import-module)
- [Basic Use Cases](#basic-use-cases)

  - [Send an email based on a Pandas DataFrame](#send-an-email-based-on-a-pandas-dataframe)
  - [Send an email out, while also appending a record into a database using Spark](#send-an-email-out-while-also-appending-a-record-into-a-database-using-spark)
  - [Send to a dedicated testing email address](#send-to-a-dedicated-testing-email-address)
  - [Parameterize your email body or email subject](#parameterize-your-email-body-or-email-subject)
  - [Do not send any email notifications](#do-not-send-any-email-notifications)
  - [How I currently use it](#how-i-currently-use-it)

# Shameless Plug
- Related Medium Post
  - Here's a [link](https://medium.com/@liam_clifford/Sparking-the-Grid-Ignite-Your-Email-Notifications-with-SendGrid-and-Spark-4a921c8f3570) to a (hopefully) less technical version of this! 

# Prerequisites
1. A SendGrid account (see setup section)
2. Access to a Spark environment

# Dependencies
- sendgrid==6.9.7
- pyspark==3.3.2
- requests==2.28.2
- numpy==1.24.2
- pandas==1.5.3


# Setup

## Sendgrid
### Generate SendGrid API Key
1. Go to [https://sendgrid.com/](https://sendgrid.com) and sign up for an account if you haven't already
2. Once you're signed in, click on the "Settings" dropdown in the left-hand navigation menu and select "API Keys"
3. Click the "Create API Key" button
4. Choose the level of access you want to grant the key. For sending emails programmatically, you'll need at least "Mail Send" permissions
5. Click the "Create & View" button
6. You'll be taken to a screen where you can view your newly created API key. 
  - Be sure to copy it down somewhere safe, as you won't be able to view it again
    - This key is required - as this is what you will be using for the `sendgrid_key` argument. 
      - **Friendly Reminder**: avoid security concerns by `not` hard coding your API keys!
        - *If you use Databricks, then you can use [Secrets](https://docs.databricks.com/security/secrets/secrets.html)*
        - *If you use Azure Data Factory, then you can use [Key Vault](https://azure.microsoft.com/en-us/products/key-vault)*


### Sender Verification
- To send emails using SendGrid, you need to verify the email address or domain from which you are sending.
- SendGrid gives you **2** options to do **Sender Verification**:

#### 1. Single Sender Verification
- You can verify a single email address by following this [step-by-step guide](https://docs.sendgrid.com/ui/sending-email/sender-verification). 
- Although SendGrid does not recommend using your personal emails for Single Sender Verification, you still can. 
  - The one thing to note here is that the emails that you send may end up in your **spam** folder initially.
- That said, if you or your company uses Google Groups, creating a dedicated Google Group email and verifying it with SendGrid could be another alternative approach to take too.

#### 2. Domain Verification
- To verify an entire domain, follow this [step-by-step video guide](https://docs.sendgrid.com/ui/account-and-settings/how-to-set-up-domain-authentication).
- **Note** that domain verification requires access to the DNS records for the domain you want to verify. You may need to work with your domain registrar or IT team to add certain DNS records to your domain's DNS settings.
- This level of verification is crucial to ensure that your emails are delivered to your recipients' inboxes and not marked as spam.
- If you choose to set up Domain Verification, it's important to note the optional `reply_to_domain` argument.
  - This feature allows you to format your emails so that the reply-to domain points back to your actual email address.
    - For instance, if you have verified `m.my_company_name.com`, you can send from any subdomain under this verified domain (e.g. **your_name@m.my_company_name.com**).
      - However, if you want people to be able to reply back to your actual email address (**your_name@my_company_name.com**), you can specify the subdomain of the email address using the `reply_to_domain` argument.



## Clone the repository:
```python
%sh rm -rf sendgrid-spark-email-automation
```
```python
%sh git clone https://github.com/liam-clifford/sendgrid-spark-email-automation.git
```
## Install the library using pip:
```python
%pip install ./sendgrid-spark-email-automation
```

## Import Module:
```python
import sendgrid_spark_email_automation
from sendgrid_spark_email_automation import *
```





# Basic Use Cases
- Once you've set up your environment and installed the dependencies, you can start using the `send_email_notification()` function to send custom email notifications.


## Send an email based on a Pandas DataFrame:

```python

email_body_template_html = "Hello World!<br><br>This is a prod email."

email_subject = "Prod Email"

sendgrid_key = 'insert_your_sendgrid_key_here'

pandas_email_df = pd.DataFrame({'to_user_emails': ['DESTINATION_EMAIL_ADDRESS_GOES_HERE'], 
                                'cc_user_emails': [''],
                                'unique_id': [1]})

send_email_notification(mode='prod',
                        pandas_email_df=pandas_email_df,
                        email_subject=email_subject, 
                        email_body_template_html=email_body_template_html,
                        from_user_email='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        sendgrid_key=sendgrid_key,
                        notification_type='hello world')
```

- This code above would send an email to the email address specified in `to_user_emails`.
- For testing purposes, there is also an optional argument called `do_not_send_any_emails`.
  - This allows you to specify whether or not you would like to try and send out an email notification. 
    - Setting this value to **True** means that it will try to send an email notification (according to your provided inputs)
    - Whereas, setting it to **False** will prevent any emails from being sent


## Send an email out, while also appending a record into a database using Spark:

```python

email_body_template_html = "Hello World!<br><br>This is a prod email."

email_subject = "Prod Email"

sendgrid_key = 'insert_your_sendgrid_key_here'

pandas_email_df = pd.DataFrame({'to_user_emails': ['DESTINATION_EMAIL_ADDRESS_GOES_HERE'], 
                                'cc_user_emails': [''],
                                'unique_id': [1]})

send_email_notification(mode='prod',
                        pandas_email_df=pandas_email_df,
                        email_subject=email_subject, 
                        email_body_template_html=email_body_template_html,
                        from_user_email='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        sendgrid_key=sendgrid_key,
                        historical_database_table='THE_DATABASE_YOU_WOULD_LIKE_TO_RECORD_YOUR_NOTIFICATIONS_IN',
                        update_historical_notification_log=True,
                        notification_type='hello world')
```

- If you would like to create an audit log of all of the emails that you send out, you will need to set the **2** following arguments:
  - `historical_database_table`: 
    - This is your hive metastore `database` and `table` that is used to tell spark the location of the database (ie. which you can think of as our email audit log) that you would like to use as a frame of reference in order to determine if we have/have not already sent an email out for the current email you are attempting to send.
      - **Notes**: 
        - if the table does not yet exist in your database, we will first create it
        - It will not create the database if the database does not yet already exist
  -  `update_historical_notification_log`: **True**
      -  This is primarily used as a debugging measure, and will allow you to explicitly specify whether or not you append a record (for each email sent) into the database specified in `historical_database_table`.


- **Bonus**: Only send **unsent** emails and avoid **duplicates** to the same recipient.
  - You can use the `skip_if_email_sent` to achieve this: 
    - Setting `skip_if_email_sent` to **True** will cross-reference our database (ie. the `historical_database_table`) to see if we have already sent an email out.   
      - The comparison is done by checking to see if we have previously sent any emails in the past (which presummably are logged in `historical_database_table`).
        - In order to do this, we want to filter out any rows (from our `pandas_email_df` variable) if the **same** combination of `to_user_emails`, `notification_type` and `unique_id` already exists as row(s) in `historical_database_table`.


## Send to a dedicated testing email address:
```python

email_body_template_html = "Hello World!<br><br>This is a test email."

email_subject = "Test Email"

sendgrid_key = 'insert_your_sendgrid_key_here'

pandas_email_df = pd.DataFrame({'to_user_emails': ['DESTINATION_EMAIL_ADDRESS_GOES_HERE'], 
                                'cc_user_emails': [''],
                                'unique_id': [1]})

send_email_notification(mode='test',
                        pandas_email_df=pandas_email_df,
                        email_subject=email_subject, 
                        email_body_template_html=email_body_template_html,
                        only_send_to_test_emails=['TESTING_EMAIL_ADDRESS_TO_RECEIVE_TEST_EMAIL'],
                        from_user_email='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        sendgrid_key=sendgrid_key,
                        historical_database_table='THE_DATABASE_YOU_WOULD_LIKE_TO_RECORD_YOUR_NOTIFICATIONS_IN',
                        notification_type='hello world')
```

- Here we are switching modes (ie mode=**'test'**), and using the `only_send_to_test_emails` argument.
- The reason we need to set mode=**'test'** is because using the `only_send_to_test_emails` argument requires us to.
  - From there, `only_send_to_test_emails` allows us to specify the list of test email(s) we want to receive **1** single email.
    - The number of testing emails that gets sent is, by default, set to **1**; however, to modify this, you can adjust the `number_of_test_records` variable.


## Parameterize your `email body` or `email subject`:

```python

email_body_template_html = "Hello World!<br>My first name is {0}.<br>My last name is {1}.<br><br>This is a test email."

email_subject = "Test Email - {0} (color)"

sendgrid_key = 'insert_your_sendgrid_key_here'

pandas_email_df = pd.DataFrame({'to_user_emails': ['DESTINATION_EMAIL_ADDRESS_GOES_HERE'], 
                                'cc_user_emails': [''],
                                'unique_id': [1],
                                'first_name': 'John',
                                'last_name': 'Doe',
                                'color': 'yellow'})

email_body_variables = ['first_name','last_name']

email_subject_variables = ['color']

send_email_notification(mode='prod',
                        pandas_email_df=pandas_email_df,
                        email_subject=email_subject, 
                        email_body_template_html=email_body_template_html,
                        email_subject_variables=email_subject_variables,
                        email_body_variables=email_body_variables,
                        from_user_email='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        sendgrid_key=sendgrid_key,
                        historical_database_table='THE_DATABASE_YOU_WOULD_LIKE_TO_RECORD_YOUR_NOTIFICATIONS_IN',
                        notification_type='hello world')
```

- Please keep in mind that we are sending a notification for each indivudal record contained in `pandas_email_df`. 

- The 2 additional arguments that we are adding are `email_subject_variables` and `email_body_variables`. Both of these arguments allow us to, for each record in our `pandas_email_df` variable, pass in dynamic arguments. 

- Here we are looping through each record in our pandas dataframe (`pandas_email_df`) and are creating a dynamic email alert according to the inputs we have provided (`email_subject_variables` and `email_body_variables`).


- In our case, `email_subject_variables` will allow us to dynamically pass in variables to our **email body**; whereas, `email_subject_variables` will allow us to pass in dynamic variables to our **email subject**. Both variables are 0-index based - meaning that the first (0) index will correspond to the {0} value that is contained in your  `email_subject_variables` or `email_subject` variables.


- Using the example above:
  - The 2 elements contained in `email_body_variables` would be dynamically passed into their corresponding index value from provided in the `email_body_template_html` variable.
  - The following: 
    - `"Hello World!<br>My first name is {0}.<br>My last name is {1}.<br><br>This is a test email."`
  - Would translate to:
    - `"Hello World!<br>My first name is `**John**`.<br>My last name is `**Doe**`.<br><br>This is a test email."`

- Similarly, this behavior would also be applied to the `subject` as well:
  - The following:
    - `"Test Email - {2}"`
  - Would translate to:
    - `"Test Email - `**yellow**` (color)"`

## Do not send any email notifications:

```python

email_body_template_html = "Hello World!<br>My first name is {0}.<br>My last name is {1}.<br><br>This is a test email."

email_subject = "Test Email"

sendgrid_key = 'insert_your_sendgrid_key_here'

pandas_email_df = pd.DataFrame({'to_user_emails': ['DESTINATION_EMAIL_ADDRESS_GOES_HERE'], 
                                'cc_user_emails': [''],
                                'unique_id': [1],
                                'first_name': 'John',
                                'last_name': 'Doe',
                                'color': 'yellow'})

email_body_variables = ['first_name','last_name']

send_email_notification(mode='test',
                        pandas_email_df=pandas_email_df,
                        email_subject=email_subject, 
                        email_body_template_html=email_body_template_html,
                        email_body_variables=email_body_variables,
                        from_user_email='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        sendgrid_key=sendgrid_key,
                        notification_type='hello world',
                        do_not_send_any_emails=True)
```

- Setting `do_not_send_any_emails` = True will prevent any emails from being sent.
  - This is a great way to debug the other functionalities of this function. 





## How I currently use it:

```python

email_body_template_html = "Hello World!<br>My first name is {0}.<br>My last name is {1}.<br><br>This is a test email."

email_subject = "Test Email - {0} (color)"

sendgrid_key = 'insert_your_sendgrid_key_here'

pandas_email_df = pd.DataFrame({'to_user_emails': ['DESTINATION_EMAIL_ADDRESS_GOES_HERE'], 
                                'cc_user_emails': [''],
                                'unique_id': [1],
                                'first_name': 'John',
                                'last_name': 'Doe',
                                'color': 'yellow'})

email_body_variables = ['first_name','last_name']

email_subject_variables = ['color']

notification_type = 'Test'

do_not_send_any_emails = False

update_historical_notification_log = True

skip_if_email_sent = True

number_of_test_records = 1

bcc_users = None

only_send_to_test_emails = None

send_email_notification(mode='prod',
                        pandas_email_df=pandas_email_df,
                        email_subject=email_subject,
                        email_body_template_html=email_body_template_html,
                        from_user_email='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        sendgrid_key=sendgrid_key,
                        notification_type='test',
                        email_subject_variables=email_subject_variables,
                        email_body_variables=email_body_variables,
                        number_of_test_records=1,
                        only_send_to_test_emails=None,
                        skip_if_email_sent=True,
                        bcc_emails=None,
                        historical_database_table='YOUR_AUTHENTICATED_EMAIL_ADDRESS_GOES_HERE',
                        update_historical_notification_log=True,
                        reply_to_domain='my_company_domain.com',
                        do_not_send_any_emails=False)
```

- To confirm, the example above does not include anything "new" in terms of what I have already covered in the other examples.
- I more so just wanted to provide an all-in-one demonstration of how I *currently* leverage the `send_email_notification()`.

#### This is also a great way to showcase the different arguments you can use:

##### Required Arguments:
- `mode`: The mode in which the function is running - 'test' or 'prod'.
- `pandas_email_df`: A Pandas DataFrame that contains the email recipients' details. 
  - It should at least have the following columns: `to_user_emails`, `cc_user_emails`, `unique_id`.
- `email_body_template_html`: The HTML string template for the email body.
- `email_subject`: The email subject.
- `from_user_email`: The email address from which the email will be sent.
- `sendgrid_key`: SendGrid API key.
- `notification_type`: A name (string) to describe the type of notification (Default: `None`).
- `update_historical_notification_log`: Whether to update the historical notification log (Default: `False`).
- `historical_database_table`: The name of the historical database table (Default: `None`).
- `number_of_test_records`: The number of test records to send (Default: `1`).
- `do_not_send_any_emails`: Whether to send any emails (Default: `False`).
- `bcc_emails`: The list of email addresses to bcc the email (Default: `None`).
- `skip_if_email_sent`: Whether to skip sending the email if it has already been sent (Default: `False`).

##### Optional Arguments:
- `email_body_variables`: A list of column names from `pandas_email_df` to use as variables in the email body template.
- `email_subject_variables`: A list of column names from `pandas_email_df` to use as variables in the email subject.
- `only_send_to_test_emails`: A list of email addresses to which the email will be sent if mode is 'test'.
- `reply_to_domain`: If you have set up Domain Authentication for your SendGrid account, you must provide this argument to ensure that recipients can reply to the correct domain.
- `do_not_cc_anyone`: Whether to cc anyone.
   
