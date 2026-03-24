import json
import boto3

EC2_INSTANCE_ID = "i-077670f8f118c7c15"

SCRIPT_PATH = "/home/ec2-user/scripts/accidents_data_pipeline.py"
AWS_REGION = "us-east-1"

ssm = boto3.client('ssm', region_name=AWS_REGION)

def lambda_handler(event, context):
    print("New S3 File Detected. Triggering Pipeline on EC2")

    command = f"runuser -l ec2-user -c 'python3 {SCRIPT_PATH}'"

    try:
        # Send command to EC2
        response = ssm.send_command(
            InstanceIds=[EC2_INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': [command]}
        )

        cmd_id = response['Command']['CommandId']
        print(f"SUCCESS: Triggered Command ID: {cmd_id}")
        return {'statusCode': 200, 'body': json.dumps("Pipeline Started")}

    except Exception as e:
        print(f"ERROR: Failed to trigger EC2. {str(e)}")
        return {'statusCode': 500, 'body': json.dumps("Error")}
