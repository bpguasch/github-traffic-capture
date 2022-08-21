#!/usr/bin/env python3

import aws_cdk as cdk

from cdk_app.cdk_app_stack import CdkAppStack


app = cdk.App()

CdkAppStack(app, "GitHubTrafficCaptureStack",)

app.synth()
