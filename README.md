# mailer

Service for processing email campaigns and bulk emails sending.
Implemented to use AWS services(ses, sqs, sns, cloudwatch ) and multiple thread workers for asynchronously call executing to balance work and increase performance when dealing with a high volume of emails.
