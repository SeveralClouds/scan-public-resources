#!/usr/bin/env python3
import os
import aws_cdk as cdk
from public_res_scanner.public_res_scanner_stack import PublicResScannerStack
from aws_cdk import DefaultStackSynthesizer


app = cdk.App()
PublicResScannerStack(
    app,
    "PublicResScannerStack",
    synthesizer=DefaultStackSynthesizer(generate_bootstrap_version_rule=False),
)
app.synth()
