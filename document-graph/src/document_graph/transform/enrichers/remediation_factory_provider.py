# Copyright (c) Evan Erwee. All rights reserved.

"""Remediation Factory Provider for AWS remediation input generation."""

import json
import hashlib
import logging
from typing import Dict, Any, List
from document_graph.transform.transformer_provider_base import TransformerProvider
from document_graph.document_graph_config import document_graph_config

logger = logging.getLogger(__name__)


class RemediationFactoryProvider(TransformerProvider):
    """Provider that generates AWS remediation inputs using S3 factory pattern."""
    
    def __init__(self, config):
        super().__init__(config)
        self.s3_bucket = self.args.get("s3_bucket")
        self.s3_prefix = self.args.get("s3_prefix", "inputs/").rstrip('/') + '/'
        self.model_id = self.args.get("model_id", "amazon.nova-pro-v1:0")
        self.source_fields = self.args.get("source_fields", ["Resource Type", "Resource external ID", "Title", "Remediation Recommendation"])
        self.target_field = self.args.get("target_field", "remediation_input")
        self.prompt_path = self.args.get("prompt_path")
        self.cloud_provider_field = self.args.get("cloud_provider_field", "Resource Platform")
        
        if not self.s3_bucket:
            raise ValueError("s3_bucket required")
        
        self.s3_client = document_graph_config._get_or_create_client("s3")
        # Add timeout configuration
        self.s3_client.meta.config.read_timeout = 30
        self.s3_client.meta.config.connect_timeout = 10
        self.bedrock_client = document_graph_config._get_or_create_client("bedrock-runtime")
        self._prompt_template = None
    
    def _get_cloud_fallback_prompt(self, cloud_provider):
        """Generate cloud-specific fallback prompt"""
        prompts = {
            "aws": "Generate AWS remediation for:\n{input_text}\nReturn JSON: {{\"service\": \"aws_service\", \"method\": \"boto3_method\", \"description\": \"fix_description\"}}",
            "azure": "Generate Azure remediation for:\n{input_text}\nReturn JSON: {{\"service\": \"azure_service\", \"method\": \"azure_method\", \"description\": \"fix_description\"}}",
            "gcp": "Generate GCP remediation for:\n{input_text}\nReturn JSON: {{\"service\": \"gcp_service\", \"method\": \"gcp_method\", \"description\": \"fix_description\"}}"
        }
        return prompts.get(cloud_provider.lower(), prompts["aws"])
    
    def _load_prompt(self, cloud_provider="aws"):
        if self._prompt_template:
            return self._prompt_template
        if self.prompt_path:
            if self.prompt_path.startswith('s3://'):
                bucket, key = self.prompt_path[5:].split('/', 1)
                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                self._prompt_template = response['Body'].read().decode('utf-8')
            else:
                with open(self.prompt_path, 'r') as f:
                    self._prompt_template = f.read()
        else:
            self._prompt_template = self._get_cloud_fallback_prompt(cloud_provider)
        return self._prompt_template
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform records by adding remediation inputs."""
        print(f"🔧 RemediationFactory: Processing {len(records)} records")
        enriched_records = []

        for record in records:
            enriched_record = record.copy()
            input_parts = [
                f"{field}: {record[field]}"
                for field in self.source_fields
                if field in record and record[field]
            ]
            if input_parts:
                input_text = "\n".join(input_parts)
                cache_key = hashlib.sha256(input_text.encode()).hexdigest()
                s3_key = f"{self.s3_prefix}{cache_key}.json"

                try:
                    response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                    enriched_record[self.target_field] = json.loads(response['Body'].read().decode('utf-8'))
                except:
                    try:
                        # Detect cloud provider from record
                        cloud_provider = "aws"  # default
                        if self.cloud_provider_field in record:
                            provider_value = str(record[self.cloud_provider_field]).lower()
                            if "azure" in provider_value or "microsoft" in provider_value:
                                cloud_provider = "azure"
                            elif "gcp" in provider_value or "google" in provider_value:
                                cloud_provider = "gcp"
                        
                        prompt = self._load_prompt(cloud_provider).format(input_text=input_text)
                        body = json.dumps({"messages": [{"role": "user", "content": [{"text": prompt}]}]})
                        response = self.bedrock_client.invoke_model(modelId=self.model_id, body=body)
                        content = json.loads(response['body'].read())['output']['message']['content'][0]['text']
                        logger.debug(f"Bedrock response: {content[:200]}...")
                        
                        # Simple JSON extraction - just find first { to last }
                        import re
                        json_start = content.find('{')
                        json_end = content.rfind('}')
                        
                        if json_start != -1 and json_end != -1:
                            json_str = content[json_start:json_end + 1]
                            # Clean control characters
                            json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_str)
                            logger.debug(f"Extracted JSON: {json_str}")
                            input_data = json.loads(json_str)
                            logger.debug(f"Storing to S3: {s3_key}")
                            try:
                                self.s3_client.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=json.dumps(input_data, indent=2))
                                logger.debug(f"Successfully stored to S3: {s3_key}")
                            except Exception as s3_error:
                                logger.error(f"S3 storage failed: {s3_error}")
                            enriched_record[self.target_field] = input_data
                            print(f"  ✅ Added {self.target_field} to record")
                        else:
                            logger.error(f"No JSON found in response: {content}")
                            raise ValueError("Could not find JSON in response")
                    except Exception as e:
                        logger.error(f"Failed to generate remediation input: {e}")
                        print(f"  ❌ Failed to add {self.target_field}: {e}")

            enriched_records.append(enriched_record)

        print(f"✅ RemediationFactory: Completed {len(enriched_records)} records")
        return enriched_records