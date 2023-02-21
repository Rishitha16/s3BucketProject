#!/usr/bin/env python
# coding: utf-8

# In[6]:


get_ipython().system('pip install boto3')


# In[8]:


import boto3
import pandas as pd

s3 = boto3.client('s3')


# In[11]:


s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='AKIA4CTBDHHGPKTCNF52',
    aws_secret_access_key='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'
)


# In[12]:


# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)


# In[13]:


get_ipython().system('pip install s3fs')


# In[14]:


import os
os.environ["AWS_DEFAULT_REGION"] = 'us-east-1'
os.environ["AWS_ACCESS_KEY_ID"] = 'AKIA4CTBDHHGPKTCNF52'
os.environ["AWS_SECRET_ACCESS_KEY"] = '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'


# In[15]:


import pandas as pd


# In[16]:


import pandas as pd

# Make dataframes
data1 = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})

# Save to csv
data1.to_csv('data1.csv')


# In[18]:


# Upload files to S3 bucket
s3.Bucket('s3bucketconnectproject').upload_file(Filename='data1.csv', Key='data1.csv')


# In[19]:


for obj in s3.Bucket('s3bucketconnectproject').objects.all():
    print(obj)


# In[20]:


# Read csv file 
obj = s3.Bucket('s3bucketconnectproject').Object('data1.csv').get()
data1 = pd.read_csv(obj['Body'], index_col=0)


# In[21]:


data1.head()


# In[22]:


data2 = pd.DataFrame({'x': [10, 20, 30], 'y': ['aa', 'bb', 'cc']})
data3 = pd.concat([data1, data2], ignore_index = True)
data3


# In[23]:


data3.to_csv('data3.csv')


# In[24]:


# Write to the s3 Bucket
test = s3.Bucket('s3bucketconnectproject').Object('data1.csv').put(Body=open('data3.csv', 'rb'))


# In[25]:


obj = s3.Bucket('s3bucketconnectproject').Object('data1.csv').get()
data4 = pd.read_csv(obj['Body'], index_col=0)
data4


# In[26]:


# Download file and read from disc
s3.Bucket('s3bucketconnectproject').download_file(Key='data1.csv', Filename='data2.csv')
pd.read_csv('data2.csv', index_col=0)


# In[36]:


client = boto3.client('s3')
metadata_bucket = client.head_bucket(
    Bucket='s3bucketconnectproject'
)


# In[37]:


print(metadata_bucket)


# In[38]:


metadata_object = client.head_object(
    Bucket='s3bucketconnectproject',
    Key='data1.csv',
) 


# In[39]:


print(metadata_object)

