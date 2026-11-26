"""Instala el disparador S3 a Glue desde AWS CloudShell."""

import base64
import hashlib
import json
import subprocess
import time
from pathlib import Path

REGION = "us-east-2"
BUCKET = "sutran-g5-datos-2026-4821"
FUNCTION = "sutran-disparador-accidentes"
ROLE = "SutranLambdaIniciarGlue"
JOB = "sutran-pipeline-completo"
RULE = "sutran-entrada-accidentes-csv"


def aws(*args, allow=None):
    result = subprocess.run(
        ["aws", *args, "--region", REGION, "--output", "json", "--no-cli-pager"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        if allow and f"({allow})" in result.stderr:
            return None
        raise SystemExit(result.stderr.strip())
    return json.loads(result.stdout) if result.stdout.strip() else {}


def main():
    package = Path(__file__).with_name("sutran_disparador_lambda.zip").resolve()
    if not package.is_file():
        raise SystemExit(f"Primero sube tambien {package.name} a CloudShell.")

    account = aws("sts", "get-caller-identity")["Account"]
    aws("s3api", "head-bucket", "--bucket", BUCKET, "--expected-bucket-owner", account)
    aws("glue", "get-job", "--job-name", JOB)
    role = aws("iam", "get-role", "--role-name", ROLE, allow="NoSuchEntity")
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    if role is None:
        role = aws(
            "iam",
            "create-role",
            "--role-name",
            ROLE,
            "--assume-role-policy-document",
            json.dumps(trust),
        )
    role_arn = role["Role"]["Arn"]
    if role["Role"]["AssumeRolePolicyDocument"] != trust:
        raise SystemExit(f"El rol {ROLE} ya existe con otra politica de confianza.")
    execution_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "glue:StartJobRun",
                "Resource": f"arn:aws:glue:{REGION}:{account}:job/{JOB}",
            },
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": [
                    f"arn:aws:logs:{REGION}:{account}:log-group:/aws/lambda/{FUNCTION}",
                    f"arn:aws:logs:{REGION}:{account}:log-group:/aws/lambda/{FUNCTION}:*",
                ],
            },
        ],
    }
    aws(
        "iam",
        "put-role-policy",
        "--role-name",
        ROLE,
        "--policy-name",
        "SutranIniciarGlue",
        "--policy-document",
        json.dumps(execution_policy),
    )

    function = aws(
        "lambda",
        "get-function-configuration",
        "--function-name",
        FUNCTION,
        allow="ResourceNotFoundException",
    )
    if function is None:
        for attempt in range(6):
            function = aws(
                "lambda",
                "create-function",
                "--function-name",
                FUNCTION,
                "--runtime",
                "python3.12",
                "--handler",
                "lambda_function.lambda_handler",
                "--role",
                role_arn,
                "--zip-file",
                f"fileb://{package}",
                "--timeout",
                "30",
                allow="InvalidParameterValueException",
            )
            if function is not None:
                break
            if attempt == 5:
                raise SystemExit("Lambda rechazo la configuracion tras esperar al rol.")
            time.sleep(5)
    digest = base64.b64encode(hashlib.sha256(package.read_bytes()).digest()).decode()
    if function["Role"] != role_arn or function["CodeSha256"] != digest:
        raise SystemExit(f"La funcion {FUNCTION} existe con un rol o codigo diferente.")
    aws("lambda", "wait", "function-active-v2", "--function-name", FUNCTION)
    arn = function["FunctionArn"]
    aws(
        "lambda",
        "add-permission",
        "--function-name",
        FUNCTION,
        "--statement-id",
        RULE,
        "--action",
        "lambda:InvokeFunction",
        "--principal",
        "s3.amazonaws.com",
        "--source-arn",
        f"arn:aws:s3:::{BUCKET}",
        "--source-account",
        account,
        allow="ResourceConflictException",
    )

    configuration = aws("s3api", "get-bucket-notification-configuration", "--bucket", BUCKET)
    rules = configuration.setdefault("LambdaFunctionConfigurations", [])
    desired = {
        "Id": RULE,
        "LambdaFunctionArn": arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": "entradas/accidentes/"},
                    {"Name": "suffix", "Value": ".csv"},
                ]
            }
        },
    }
    existing = next((rule for rule in rules if rule.get("Id") == RULE), None)
    if existing and existing != desired:
        raise SystemExit(f"La regla {RULE} ya existe con otra configuracion.")
    if existing is None:
        rules.append(desired)
        aws(
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            BUCKET,
            "--expected-bucket-owner",
            account,
            "--notification-configuration",
            json.dumps(configuration),
        )
    actual = aws("s3api", "get-bucket-notification-configuration", "--bucket", BUCKET)
    matching = [
        rule for rule in actual.get("LambdaFunctionConfigurations", []) if rule["Id"] == RULE
    ]
    if not matching:
        raise SystemExit("S3 no devolvio la regla guardada.")
    print(json.dumps(matching[0], indent=2))
    print("DISPARADOR_SUTRAN_OK")


if __name__ == "__main__":
    main()
