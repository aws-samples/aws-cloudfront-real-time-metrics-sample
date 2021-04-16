## CloudWatch Metrics from CloudFront Real-Time Logs

This repository contains sample code to create infrastructure to parse [CloudFront real-time logs](https://aws.amazon.com/about-aws/whats-new/2020/08/cloudfront-realtimelogs/). This infrastructure will use data from CloudFront real-time logs and upload it to [CloudWatch](https://aws.amazon.com/cloudwatch/) as custom metrics. These custom metrics in CloudWatch can provide low-level insight into how well a CloudFront distribution is performing and even enable monitoring the operational health of [specific CloudFront edge locations](https://aws.amazon.com/cloudfront/features/). This can assist in the identification of common problems affecting CloudFront distributions such as localized outages, ISP failures, and DNS routing issues. This code is used as part of a an AWS blog, which can be referenced here.

![Overview](/images/overview.png)

## Deploying the Code

This repository is intended to be a companion to an AWS blog. You should review this blog prior to using code from this repository. An AWS SAM template in this repository provides deployment capabilities for this project. To deploy this project, follow the steps below:
1. Clone the source code from this repository to your local environment
2.	From a shell prompt in your local environment, change directory to the root of the source code cloned from this respository
3. Run the command below to trigger the stack creation dialog

    `sam deploy --template cfn/template.yaml --guided`

4. Populate the **Stack Name** parameter with any valid CloudFormation stack name. The **AWS Region** parameter can be any valid AWS Region, but it will default to us-east-1. Provide values for the **CloudFrontDistributionDomainName** and **KinesisShardCount** parameters as appropriate. You must respond with a “y” when prompted to **Allow SAM CLI IAM role creation** or else the deployment will fail. Responding with a “y” to the **Confirm changes before deploy** will allow you to review a CloudFormation changeset prior to the deployment. Respond with a “y” to **Save arguments to a configuration file** to prevent being asked these questions in the future.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

